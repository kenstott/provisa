# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e8a47-2f9b-4d3e-8b16-7a0d4e9c3f52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""bytea LIKE on openGauss, matched byte-wise (REQ-2236, amended 2026-11-24).

openGauss's bytealike and byteanlike (src/common/backend/utils/adt/like.cpp) call GenericMatchText,
which on a UTF-8 database matches the bytes as multibyte text: a value ending in a lead byte with
too few bytes after it is read past its end, so the answer varies from run to run. like, notlike,
bytealike, byteanlike and the ~~ and !~~ operators on bytea all reach it. openGauss's own rawlike
and rawnlike match byte-wise (SB_MatchText), as PostgreSQL's bytea LIKE does, so each such call is
written as rawlike(hextoraw(encode(value, 'hex')), hextoraw(encode(pattern, 'hex'))), or rawnlike
for the negated forms. openGauss has no cast from bytea to raw ("cannot cast type bytea to raw");
hextoraw of the value's hex digits is a raw with the same bytes, an empty bytea gives an empty raw,
and a NULL stays NULL (shown live by tests/integration/test_opengauss_bytea_like.py).

openGauss keeps the postgres dialect; this step runs for the openGauss engine kind only.
"""

from __future__ import annotations

import sqlglot.expressions as exp

KIND = "opengauss"

# The bytea-only functions, negated or not.
_BYTEA_ONLY = {"bytealike": False, "byteanlike": True}
# The functions with a bytea overload among others, chosen by a bytea operand.
_OVERLOADED = {"like": False, "notlike": True}


def mentioned(pg_sql: str) -> bool:
    """Whether ``pg_sql`` may hold a LIKE this step rewrites: a cheap text test before a parse."""
    lowered = pg_sql.lower()
    return "like" in lowered or "~~" in lowered


def _is_bytea(node: exp.Expr) -> bool:
    node = node.unnest()
    return isinstance(node, (exp.Cast, exp.TryCast)) and node.to.is_type(
        exp.DType.VARBINARY, exp.DType.BINARY
    )


def _as_raw(node: exp.Expr) -> exp.Expr:
    """openGauss's raw with the bytes of bytea ``node``: hextoraw(encode(node, 'hex'))."""
    hex_digits = exp.Anonymous(this="encode", expressions=[node.copy(), exp.Literal.string("hex")])
    return exp.Anonymous(this="hextoraw", expressions=[hex_digits])


def _raw_match(value: exp.Expr, pattern: exp.Expr, negated: bool) -> exp.Expr:
    return exp.Anonymous(
        this="rawnlike" if negated else "rawlike",
        expressions=[_as_raw(value), _as_raw(pattern)],
    )


def _call(node: exp.Expr) -> tuple[exp.Anonymous, bool] | None:
    """The bytea LIKE function ``node`` calls, bare or pg_catalog-qualified, and whether it is
    negated; None when ``node`` is no such call."""
    called = node.expression if isinstance(node, exp.Dot) else node
    if isinstance(node, exp.Dot) and node.this.name.lower() != "pg_catalog":
        return None
    if not isinstance(called, exp.Anonymous) or len(called.expressions) != 2:
        return None
    name = called.name.lower()
    if name in _BYTEA_ONLY:
        return called, _BYTEA_ONLY[name]
    if name in _OVERLOADED and any(_is_bytea(a) for a in called.expressions):
        return called, _OVERLOADED[name]
    return None


def _rewritten(node: exp.Expr) -> exp.Expr:
    if isinstance(node, exp.Like) and node.args.get("escape") is None:
        if _is_bytea(node.this) or _is_bytea(node.expression):
            return _raw_match(node.this, node.expression, bool(node.args.get("negate")))
        return node
    if isinstance(node, exp.Not) and isinstance(node.this, exp.Like):
        inner = node.this
        if inner.args.get("escape") is None and (
            _is_bytea(inner.this) or _is_bytea(inner.expression)
        ):
            return _raw_match(inner.this, inner.expression, not inner.args.get("negate"))
        return node
    found = _call(node)
    if found is None:
        return node
    called, negated = found
    value, pattern = called.expressions
    return _raw_match(value, pattern, negated)


def rewritten(node: exp.Expr, _dialect: str) -> exp.Expr | None:
    """A LIKE on bytea written as openGauss's byte-wise rawlike or rawnlike.

    None for any other node. The form of the grid's hint of openGauss
    (provisa.transpiler.polyfill.hints)."""
    written = _rewritten(node)
    return None if written is node else written


def byte_wise(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each LIKE on bytea written as openGauss's byte-wise rawlike or rawnlike: the
    grid's hint of openGauss (provisa.transpiler.polyfill.hints), applied to every node."""
    return tree.transform(lambda node: rewritten(node, "postgres") or node, copy=False)
