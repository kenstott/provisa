# Copyright (c) 2026 Kenneth Stott
# Canary: 36203e95-fe6b-4d15-bbbb-573eef35dcc6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's proxy (REQ-2100, REQ-1756): the one thing in Provisa that calls a code
table's function.

Anything that reaches a code source is single-source: a direct read of its tables, or the part of
a statement an engine pushes to it. So a code source answers its own statements: each table's
function is called with its read's arguments, and DuckDB applies the governed statement to the
rows (codesource.direct for a direct read, the image's pgwire front end for the part an engine
pushes). Its plan of a read of its own tables is the complete dataset; nothing is landed. A
statement across sources is the engine's, which attaches a built source's image and pushes its
part, or lands a dev-mode source's rows (its call made once per argument set, the rows checked
against the table's declared columns)."""

from __future__ import annotations

from typing import Any

from provisa.federation.connector_contract import Plan, Read, SourceContract


class CodeProxy(SourceContract):
    async def plan(self, state: Any, read: Read, ctx: Any = None) -> Plan:
        from provisa.codesource.table import pushed_arguments

        # The function is given its read's arguments and the values the statement's predicates
        # give the parameters it declares a pushdown for. A statement that reads this source's
        # tables alone (routing's one_source gate) is answered whole by DuckDB over their rows.
        pushed = await pushed_arguments(state, read.table, read.name, read.statement)
        return Plan(
            read=read, arguments={**pushed, **(read.arguments or {})}, complete_dataset=True
        )

    async def call(self, state: Any, plan: Plan) -> Any:
        from provisa.codesource.table import read

        return await read(state, plan.read.table, plan.arguments)

    async def call_each(self, state: Any, table: dict, argument_sets: list, *, joined: bool) -> Any:
        """REQ-2099: a join whose relationship feeds the table's parameters calls the function
        once per distinct set of the parent's values, in one request to its worker."""
        from provisa.codesource.table import read_each

        return await read_each(state, table, argument_sets, joined=joined)
