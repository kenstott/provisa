# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7b5c94-8d16-4f3a-9c20-6a1e4d8b3f57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A SQL expression function called on its own (REQ-2168): GraphQL, REST, MCP or a Cypher CALL
name it as a command. Its template is SELECTed with the call's arguments bound as the statement's
parameters (``$1`` … in declared order), through the one pipeline as the caller's role. The one
row it answers holds the value as ``value``."""

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp

from provisa.core.sql_expression import render


def statement(fn: dict) -> str:
    """``SELECT <template> AS value``, each argument the parameter of its declared position."""
    params: list[exp.Expression] = [
        exp.Parameter(this=exp.Literal.number(i + 1)) for i in range(len(fn["arguments"]))
    ]
    return exp.select(exp.alias_(render(fn, params), "value", quoted=True)).sql(dialect="postgres")


async def run(fn: dict, args: dict, state: Any, role_id: str | None) -> list[dict]:
    from provisa.executor.serialize import _convert_value
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    if role_id is None:
        raise ValueError(f"function {fn['name']!r} runs as a role; none is bound")
    params = [args[a["name"]] for a in fn["arguments"]]
    plan = await _govern_and_route(statement(fn), role_id, params=params or None)
    result = await _execute_plan(plan, state)
    return [
        {c: _convert_value(v) for c, v in zip(result.column_names, r, strict=True)}
        for r in result.rows
    ]
