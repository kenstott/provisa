# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2a8f71-4c06-4e9b-b3a7-1f6e9c2d8a54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL expression functions written in a SQL statement (REQ-2168): each call of one the role may
use, named by its SQL command binding, is replaced by its template with the call's arguments put
in, before the statement is governed, so governance reads one statement and the function pushes
down with it.

A statement naming no such function costs one substring test per function name: the functions
by SQL name are read once per registry, and the tree is walked only when a name is in the text.
"""

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

# id(registry) -> (registry, {sql name (lower): function}); one entry per live registry.
_BY_REGISTRY: dict[int, tuple[Any, dict[str, dict]]] = {}
_MAX_REGISTRIES = 64


def _functions(state: object) -> dict[str, dict]:  # object-ok: AppState lives above compiler
    """The SQL expression functions by their SQL name, read once per registry."""
    from provisa.core.command_bindings import offered_name

    registry = getattr(state, "tracked_functions", None) or {}
    held = _BY_REGISTRY.get(id(registry))
    if held is not None and held[0] is registry:
        return held[1]
    by_name = {
        offered_name(f, "sql").lower(): f
        for f in registry.values()
        if f.get("impl_kind") == "sql_expression"
    }
    if len(_BY_REGISTRY) >= _MAX_REGISTRIES:
        _BY_REGISTRY.pop(next(iter(_BY_REGISTRY)))
    _BY_REGISTRY[id(registry)] = (registry, by_name)
    return by_name


def expand(tree: exp.Expression, raw_sql: str, role_id: str, state: object) -> bool:
    """Replace, in place, each call in ``tree`` of a SQL expression function ``role_id`` may use.
    True when one was replaced."""
    functions = _functions(state)
    if not functions:
        return False
    text = raw_sql.lower()
    named = [name for name in functions if name in text]
    if not named:
        return False
    from provisa.core.sql_expression import refuse_unless_writable, render
    from provisa.security.mutation_authz import command_reachable
    from provisa.security.rights import require_role

    role = require_role(getattr(state, "roles"), role_id)
    usable = {n: functions[n] for n in named if command_reachable(functions[n], role)}
    calls = [
        node
        for node in tree.find_all(exp.Anonymous)
        if node.name.lower() in usable and not isinstance(node.parent, exp.Table)
    ]
    for node in calls:  # REQ-2168: refused by name where the engine cannot write it
        refuse_unless_writable(usable[node.name.lower()], node.name, state)
    for node in reversed(calls):  # the innermost first: an outer call copies its arguments
        node.replace(render(usable[node.name.lower()], list(node.expressions)))
    return bool(calls)
