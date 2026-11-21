# Copyright (c) 2026 Kenneth Stott
# Canary: c4238b2e-de8f-438c-9e2a-369f14281d14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered timestamptz column that to_json reads, said to be one (REQ-2171).

Trino refuses a cast of an instant to JSON while it types the statement, so to_json of an instant
has a form there only where the statement itself says the value is one
(``transpiler.zoned_instants``). The registry knows a column is a timestamptz: each such column
inside a to_json or to_jsonb call is cast to the type it already has, which changes no answer and
says it. This runs in both physical rewrites, beside the subtype markers.
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.native_subtype import SubtypeColumns, _resolve

INSTANT = "timestamptz"
_JSON_CALLS = frozenset({"to_json", "to_jsonb"})


def _said(col: exp.Column) -> bool:
    parent = col.parent
    return isinstance(parent, exp.Cast) and parent.to.is_type(exp.DataType.Type.TIMESTAMPTZ)


def state_json_instants(sql: str, instant_columns: SubtypeColumns) -> str:
    """``sql`` (PostgreSQL dialect, physical table names) with each column of
    ``instant_columns`` inside a to_json or to_jsonb call cast to timestamptz. Idempotent, and a
    statement that calls neither or names no such table is returned as given, unparsed."""
    if not instant_columns:
        return sql
    lowered = sql.lower()
    if "to_json" not in lowered or not any(table in lowered for (_s, table) in instant_columns):
        return sql
    from provisa.compiler.sql_rewrite import _scope_tables

    tree = sqlglot.parse_one(sql, read="postgres")
    scopes: dict[int, dict[str, tuple[str, str]]] = {}
    changed = False
    for call in list(tree.find_all(exp.Anonymous)):
        if call.name.lower() not in _JSON_CALLS:
            continue
        for col in list(call.find_all(exp.Column)):
            if isinstance(col.this, exp.Star) or _said(col):
                continue
            scope = col.find_ancestor(exp.Select)
            if scope is None:
                continue
            tables = scopes.setdefault(id(scope), _scope_tables(scope))
            if _resolve(col, tables, instant_columns) != INSTANT:
                continue
            col.replace(
                exp.Cast(this=col.copy(), to=exp.DataType.build(INSTANT, dialect="postgres"))
            )
            changed = True
    return tree.sql(dialect="postgres") if changed else sql


__all__ = ["INSTANT", "state_json_instants"]
