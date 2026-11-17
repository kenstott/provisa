# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source operation's function called on its own (REQ-1986): GraphQL, REST, MCP or a Cypher
CALL name it as a command. It is read as SQL reads it, ``SELECT * FROM fn(name => $1, ...)``,
each given argument bound as a parameter, through the one pipeline as the caller's role: the
same relation as its table with those arguments assigned, governed as that table is."""

# Requirements: REQ-1986

from __future__ import annotations

from typing import Any

import sqlglot.expressions as exp


def statement(fn: dict, given: list[str]) -> str:
    """``SELECT * FROM <sql name>(<arg> => $1, ...)`` for the arguments ``given``, in order."""
    from provisa.core.command_bindings import offered_name

    call = exp.Anonymous(
        this=offered_name(fn, "sql"),
        expressions=[
            exp.Kwarg(
                this=exp.to_identifier(name, quoted=True),
                expression=exp.Parameter(this=exp.Literal.number(i + 1)),
            )
            for i, name in enumerate(given)
        ],
    )
    return exp.select(exp.Star()).from_(exp.Table(this=call)).sql(dialect="postgres")


async def run(fn: dict, args: dict, state: Any, role_id: str | None) -> list[dict]:
    from provisa.executor.serialize import result_records
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    if role_id is None:
        raise ValueError(f"function {fn['name']!r} runs as a role; none is bound")
    given = [name for name, value in args.items() if value is not None]
    plan = await _govern_and_route(
        statement(fn, given), role_id, params=[args[n] for n in given] or None
    )
    result = await _execute_plan(plan, state)
    return result_records(result)
