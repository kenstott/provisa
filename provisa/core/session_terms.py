# Copyright (c) 2026 Kenneth Stott
# Canary: b17d4a02-93e6-4c58-8e2f-1a6c0f7d3b45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session variable that holds several values, read with IN (REQ-2188).

A session variable is one text value, and ``current_setting('provisa.<name>')`` resolves to one
literal. ``acting_roles`` is several: the roles a caller acts as. A rule reads it the way it reads
any variable, as one member of an IN list::

    owner_role IN (current_setting('provisa.acting_roles'))

and the term is expanded here, on the tree, to one literal per value, so the same rule runs on
every engine and in the model store. Read outside an IN list it is the comma-joined text, which
is the one role for a caller acting as one. An unbound variable is left alone: it resolves to
NULL as every unbound variable does (REQ-1682), and the rule matches nothing.
"""

# Requirements: REQ-2188, REQ-1682

from __future__ import annotations

import re

import sqlglot.expressions as exp

from provisa.core.request_context import ACTING_ROLES_VAR

#: The variables holding several values, joined by a comma. A role id holds no comma: a role header
#: names a set of them, comma-separated (security.meta_role).
LIST_SETTINGS = frozenset({ACTING_ROLES_VAR})

_SETTING_NAME = re.compile(r"^provisa\.([A-Za-z0-9_]+)$")


def setting_name(node: exp.Expression) -> str | None:
    """The variable ``current_setting('provisa.<name>')`` reads, or None for any other term. A
    cast the column's type put round the term (REQ-1686) is seen through."""
    term = node.this if isinstance(node, exp.Cast) else node
    if not isinstance(term, exp.Anonymous) or term.name.lower() != "current_setting":
        return None
    arguments = term.expressions
    if not arguments or not isinstance(arguments[0], exp.Literal):
        return None
    match = _SETTING_NAME.match(arguments[0].name)
    return None if match is None else match.group(1)


def _value(term: exp.Expression, text: str) -> exp.Expression:
    literal = exp.Literal.string(text)
    return (
        exp.Cast(this=literal, to=term.args["to"].copy()) if isinstance(term, exp.Cast) else literal
    )


def expand_list_settings(tree: exp.Expression, session_vars: dict[str, str]) -> None:
    """Replace, in place, each list variable a bound IN list reads with one literal per value."""
    for node in list(tree.find_all(exp.In)):
        expanded: list[exp.Expression] = []
        changed = False
        for term in node.expressions:
            name = setting_name(term)
            value = session_vars.get(name) if name is not None and name in LIST_SETTINGS else None
            if value is None:
                expanded.append(term)
                continue
            changed = True
            expanded.extend(_value(term, v) for v in value.split(",") if v)
        if changed:
            node.set("expressions", expanded)


def resolve_session_terms(tree: exp.Expression, session_vars: dict[str, str]) -> exp.Expression:
    """``tree`` with every ``current_setting`` term a literal, NULL when unbound: for a predicate
    run where the function does not exist (the model store, on any backend)."""
    expand_list_settings(tree, session_vars)

    def literal(node: exp.Expression) -> exp.Expression:
        name = setting_name(node)
        if name is None or isinstance(node, exp.Cast):
            return node
        value = session_vars.get(name)
        return exp.null() if value is None else exp.Literal.string(value)

    return tree.transform(literal)
