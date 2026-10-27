# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3a9c52-1e86-4d0b-b5a4-2c9e8d6f1a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Commands and functions a Cypher statement names by their Cypher command bindings (REQ-2168),
resolved for the acting role before the statement's SQL is governed.

* A function declared as a SQL expression template, called in an expression
  (``RETURN apoc.text.join(list, ',')``), is replaced by its template with the call's arguments
  put in, so it pushes down with the statement and runs under its governance. A namespaced
  function name that is no function the role may use is refused by name.
* A command called in a statement (``CALL apoc.meta.data() YIELD label, type WHERE …``) is
  translated to the marker table function (``CALL_MARKER``); here it runs through the one command
  executor as the role, and its rows, shaped by the YIELD, take the marker's place as a
  ``VALUES`` relation -- as a command composed in SQL is put in place (REQ-1159).
"""

from __future__ import annotations

import json
from typing import Any, TypeVar, cast

import sqlglot
import sqlglot.expressions as exp

from provisa.api.errors import ApiError
from provisa.gql.translator_types import CALL_MARKER


def _bound(state: Any, role_id: str) -> dict[str, dict]:
    from provisa.api.data.action_exec import usable_commands

    return usable_commands(state, role_id, language="cypher")


_E = TypeVar("_E", bound=exp.Expression)


def expand_functions(sql_ast: _E, role_id: str, state: Any) -> _E:
    """``sql_ast`` with each call of a Cypher-bound SQL expression function replaced by its
    template. A namespaced call that names no such function is refused by name."""
    from provisa.core.sql_expression import SqlExpressionRefused, refuse_unless_writable, render

    calls = [
        n
        for n in sql_ast.find_all(exp.Anonymous)
        if "." in n.name and n.name.upper() != CALL_MARKER.upper()
    ]
    if not calls:
        return sql_ast
    bound = _bound(state, role_id)
    functions = {
        name.lower(): fn for name, fn in bound.items() if fn.get("impl_kind") == "sql_expression"
    }

    def _put(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Anonymous) or "." not in node.name:
            return node
        if node.name.upper() == CALL_MARKER.upper():
            return node
        fn = functions.get(node.name.lower())
        if fn is None:
            raise ApiError(
                400,
                "cypher.unknown_function",
                f"Unknown function: {node.name!r}",
                name=node.name,
            )
        try:
            refuse_unless_writable(fn, node.name, state)
            return render(fn, list(node.expressions))
        except SqlExpressionRefused as exc:
            raise ApiError(
                400, "cypher.function_call_refused", str(exc), name=node.name, reason=str(exc)
            ) from exc

    return cast(_E, sql_ast.transform(_put))


def calls_commands(sql: str) -> bool:
    """Whether the statement holds a composed command call to put in place."""
    return CALL_MARKER.lower() in sql.lower()


def _sql_type(values: list[Any]) -> exp.DataType:
    """The type of a column the command declares no type for, read off its values."""
    present = [v for v in values if v is not None and v != []]
    sample = present[0] if present else None
    if isinstance(sample, bool):
        return exp.DataType.build("BOOLEAN")
    if isinstance(sample, int):
        return exp.DataType.build("BIGINT")
    if isinstance(sample, float):
        return exp.DataType.build("DOUBLE")
    if isinstance(sample, dict):
        return exp.DataType.build("JSON")
    if isinstance(sample, list):
        items = [i for v in present for i in v]
        return exp.DataType(
            this=exp.DataType.Type.ARRAY, expressions=[_sql_type(items)], nested=True
        )
    return exp.DataType.build("VARCHAR")


def _cell(value: Any) -> exp.Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, (int, float)):
        return exp.Literal.number(value)
    if isinstance(value, dict):
        return exp.Literal.string(json.dumps(value, default=str))
    if isinstance(value, list):
        return exp.Array(expressions=[_cell(v) for v in value])
    return exp.Literal.string(str(value))


def _declared_types(command: dict) -> dict[str, exp.DataType]:
    from provisa.core.ir_types import to_physical

    return {
        c["name"]: exp.DataType.build(to_physical(c["type"], "postgresql"), dialect="postgres")
        for c in command.get("output_columns") or []
    }


def _relation(command: dict, rows: list[dict], yields: list[str], alias: exp.TableAlias):
    """The rows as a typed relation of the yielded columns, under the call's own alias."""
    declared = _declared_types(command)
    types = [declared.get(c) or _sql_type([r.get(c) for r in rows]) for c in yields]
    if not rows:
        empty = exp.select(*[exp.Cast(this=exp.Null(), to=t) for t in types]).where(exp.false())
        return exp.Subquery(this=empty, alias=alias)
    tuples = [
        exp.Tuple(
            expressions=[
                exp.Cast(this=_cell(row.get(c)), to=t) if i == 0 else _cell(row.get(c))
                for c, t in zip(yields, types, strict=True)
            ]
        )
        for i, row in enumerate(rows)
    ]
    return exp.Values(expressions=tuples, alias=alias)


async def put_command_rows(sql: str, role_id: str, state: Any) -> str:
    """``sql`` with each composed command call replaced by the command's rows, the command run
    once per call through the one executor as ``role_id``."""
    from provisa.api.data.action_exec import (
        bind_command_args,
        command_named,
        invoke_tracked_function,
    )

    tree = sqlglot.parse_one(sql, read="postgres")
    for table in list(tree.find_all(exp.Table)):
        marker = table.this
        if not isinstance(marker, exp.Anonymous) or marker.name.upper() != CALL_MARKER.upper():
            continue
        called, yields_json, values_json = (a.this for a in marker.expressions)
        yields: list[str] = json.loads(yields_json)
        name = command_named(state, role_id, "cypher", called)
        args = bind_command_args(name, json.loads(values_json), state, role_id)
        rows = await invoke_tracked_function(name, args, state, role_id)
        returned = set(rows[0]) if rows else set(yields)
        missing = [c for c in yields if c not in returned]
        if missing:
            raise ApiError(
                400,
                "cypher.command_call_refused",
                f"YIELD after CALL {called}: {missing[0]!r} is not a column it returns "
                f"({', '.join(sorted(returned))})",
                reason="yield_not_returned",
            )
        command = state.tracked_functions.get(name) or state.tracked_webhooks[name]
        table.replace(_relation(command, rows, yields, table.args["alias"].copy()))
    return tree.sql(dialect="postgres")
