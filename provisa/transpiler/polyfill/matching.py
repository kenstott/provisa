# Copyright (c) 2026 Kenneth Stott
# Canary: ef7ccae2-bd43-4aa6-8a25-5654ec51d112
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Applying a rule (REQ-2236): a pattern's tree matched against a statement's node, tree against
tree, and the subtrees its holes matched grafted into a replacement's tree.

A node matches a pattern when it is the same kind of node with the same arguments, a hole matching
any subtree and a typed hole a subtree whose said type fits its type. A NULL cast to a type in a
pattern (``NULL::anycompatiblearray``) matches a NULL whose said type fits that type. A hole written
twice matches the same subtree twice. A node that does not match is not the rule's: nothing is
guessed."""

# Requirements: REQ-2236

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

from provisa.pgwire import function_types
from provisa.transpiler.polyfill.notation import RuleError, hole_name, typed_hole

#: A hole's name -> the subtree of the statement it matched.
Bindings = dict[str, exp.Expr]


def match(pattern: exp.Expr, node: exp.Expr) -> Bindings | None:
    """The subtrees ``node`` binds to ``pattern``'s holes, or None where it does not match."""
    bindings: Bindings = {}
    return bindings if _matches(pattern, node, bindings) else None


#: The catalog's vector types, which PostgreSQL reads as the array of their element.
_VECTORS = {"int2vector": "smallint[]", "oidvector": "oid[]"}
#: PostgreSQL's implicit casts between numeric types: what a value of each widens to.
_WIDENS = {
    "smallint": frozenset({"integer", "bigint", "numeric", "real", "double precision"}),
    "integer": frozenset({"bigint", "numeric", "real", "double precision"}),
    "bigint": frozenset({"numeric", "real", "double precision"}),
    "numeric": frozenset({"real", "double precision"}),
    "real": frozenset({"double precision"}),
}


def fits(node: exp.Expr, sql_type: str) -> bool:
    """Whether the type a statement says of ``node`` fits ``sql_type``, as function_types fits an
    argument to a parameter: a catalog vector is its array, and a number widens as PostgreSQL's
    implicit casts widen it."""
    said = function_types.argument_type(node)
    said = _VECTORS.get(said or "", said)
    return function_types.fits(said, sql_type) or sql_type in _WIDENS.get(said or "", ())


def _bound(name: str, node: exp.Expr, bindings: Bindings) -> bool:
    seen = bindings.get(name)
    if seen is None:
        bindings[name] = node
        return True
    return seen == node


def _matches(pattern: exp.Expr, node: exp.Expr, bindings: Bindings) -> bool:
    typed = typed_hole(pattern)
    if typed is not None:
        name, sql_type = typed
        return fits(node, sql_type) and _bound(name, node, bindings)
    name = hole_name(pattern)
    if name is not None:
        return _bound(name, node, bindings)
    if isinstance(pattern, exp.Cast) and isinstance(pattern.this, exp.Null):
        said = function_types.argument_type(pattern)
        if said is None:
            raise RuleError(f"the type {pattern.to.sql('postgres')} is not one PostgreSQL has")
        return _is_null(node) and fits(node.unnest(), said)
    if type(pattern) is not type(node):
        return False
    if isinstance(pattern, exp.Anonymous):
        return pattern.name.lower() == node.name.lower() and _all_match(
            pattern.expressions, node.expressions, bindings
        )
    if isinstance(pattern, exp.Identifier) and isinstance(node, exp.Identifier):
        if pattern.quoted != node.quoted:
            return False
        # An identifier not quoted is folded, as PostgreSQL folds it.
        return pattern.name == node.name or (
            not pattern.quoted and pattern.name.lower() == node.name.lower()
        )
    keys = {k for k, v in pattern.args.items() if not _absent(v)}
    if keys != {k for k, v in node.args.items() if not _absent(v)}:
        return False
    return all(_value_matches(pattern.args[k], node.args[k], bindings) for k in keys)


def _is_null(node: exp.Expr) -> bool:
    """Whether ``node`` is a NULL, of a type said or not."""
    node = node.unnest()
    if isinstance(node, exp.Cast):
        node = node.this.unnest()
    return isinstance(node, exp.Null)


def _absent(value: Any) -> bool:
    return value is None or (isinstance(value, list) and not value)


def _all_match(patterns: list[Any], nodes: list[Any], bindings: Bindings) -> bool:
    return len(patterns) == len(nodes) and all(
        _value_matches(p, n, bindings) for p, n in zip(patterns, nodes)
    )


def _value_matches(pattern: Any, node: Any, bindings: Bindings) -> bool:
    if isinstance(pattern, exp.Expr):
        return isinstance(node, exp.Expr) and _matches(pattern, node, bindings)
    if isinstance(pattern, list):
        return isinstance(node, list) and _all_match(pattern, node, bindings)
    return pattern == node


def graft(replacement: exp.Expr, bindings: Bindings) -> exp.Expr:
    """A copy of ``replacement`` with each hole the subtree bound to it, copied."""

    def put(node: exp.Expr) -> exp.Expr:
        name = hole_name(node)
        if name is None:
            return node
        if name not in bindings:
            raise RuleError(f"${name} is bound to nothing")
        return bindings[name].copy()

    # transform does not visit what it put: a bound subtree is the statement's, never the rule's.
    return replacement.copy().transform(put)
