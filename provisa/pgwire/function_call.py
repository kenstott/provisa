# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4b2c71-6a09-4f53-9e12-3c7a0d6f8b45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pgwire SELECT-of-a-registered-function binding to the shared executor (REQ-872).

Detects a bare ``SELECT * FROM fn(args)`` / ``SELECT fn(args)`` whose ``fn`` is a
registered tracked function, coerces its literal arguments, and adapts the executor's
row dicts back to a pgwire QueryResult. Invocation routes through the one shared
``invoke_tracked_function`` executor, which enforces per-mutation writable_by.
"""

from __future__ import annotations

from provisa.executor.result import QueryResult


def _literal_value(node):
    """Coerce a sqlglot argument node to a Python value (string/number/bool/null)."""
    import sqlglot.expressions as exp

    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        text = node.this
        try:
            return int(text)
        except ValueError:
            return float(text)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Null):
        return None
    return node.sql()  # fall back to the rendered SQL for anything exotic


def detect_sql_function_call(sql: str, state) -> tuple[str, list] | None:
    """Return (registered function name, positional arg values) for a function-call SELECT.

    Handles ``SELECT * FROM fn(args)`` and ``SELECT fn(args)``; returns None when the SQL
    is a normal query or names no registered tracked function.
    """
    fns = getattr(state, "tracked_functions", None)
    if not isinstance(fns, dict):
        return None
    import sqlglot
    import sqlglot.expressions as exp
    from sqlglot.errors import SqlglotError

    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except (SqlglotError, RecursionError):
        return None
    for node in tree.find_all(exp.Anonymous):
        if node.name in fns:
            return node.name, [_literal_value(a) for a in node.expressions]
    return None


def rows_to_query_result(rows: list[dict]) -> QueryResult:
    """Adapt the executor's row dicts to a pgwire QueryResult (column-ordered tuples)."""
    if not rows:
        return QueryResult(rows=[], column_names=[])
    cols = list(rows[0].keys())
    return QueryResult(rows=[tuple(r.get(c) for c in cols) for r in rows], column_names=cols)


async def maybe_invoke_registered_function(sql: str, role_id: str, state):
    """If *sql* is a registered-function-call SELECT, run it via the shared executor.

    Returns a QueryResult, or None to signal the caller should fall through to normal
    governance/routing. writable_by is enforced inside the executor (REQ-869).
    """
    hit = detect_sql_function_call(sql, state)
    if hit is None:
        return None
    from provisa.api.data.action_exec import invoke_tracked_function

    name, values = hit
    args = {f"a{i}": v for i, v in enumerate(values)}
    rows = await invoke_tracked_function(name, args, state, role_id)
    return rows_to_query_result(rows)
