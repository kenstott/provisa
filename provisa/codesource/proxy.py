# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's proxy (REQ-2100, REQ-1756): the one thing in Provisa that calls a code
table's function. Its plan gives the function its read's arguments and the values the statement's
predicates give the parameters the function declares a pushdown for (codesource.pushdown); its
call is made once per argument set, in the source's worker or its image, and the rows are checked
against the table's declared columns. v1: a code source declares no capability, so its rows are
never the complete dataset: they are landed, and the engine runs the statement over them."""

from __future__ import annotations

from typing import Any

from provisa.federation.connector_contract import Plan, Read, SourceContract


class CodeProxy(SourceContract):
    async def plan(self, state: Any, read: Read, ctx: Any = None) -> Plan:
        from provisa.codesource.table import pushed_arguments

        pushed = await pushed_arguments(state, read.table, read.name, read.statement)
        return Plan(
            read=read,
            arguments={**pushed, **(read.arguments or {})},
            incomplete_because="a code source declares no capability",
        )

    async def call(self, state: Any, plan: Plan) -> Any:
        from provisa.codesource.table import read

        return await read(state, plan.read.table, plan.arguments)

    async def call_each(self, state: Any, table: dict, argument_sets: list, *, joined: bool) -> Any:
        """REQ-2099: a join whose relationship feeds the table's parameters calls the function
        once per distinct set of the parent's values, in one request to its worker."""
        from provisa.codesource.table import read_each

        return await read_each(state, table, argument_sets, joined=joined)
