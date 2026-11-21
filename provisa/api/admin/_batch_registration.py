# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2d6e41-7a3c-4f08-8c51-e4a7b9d2f013
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Several tables of a source registered in one action, from Register Table (REQ-2234).

Each table is registered by the registerTable mutation, with the same rights and checks as
registering it alone: there is no second way to register a table. A table one of those checks
refuses is answered by name, with what refused it, and the others are registered.
"""

# Requirements: REQ-2234

from __future__ import annotations

from typing import Any

from provisa.api.admin.types import MutationResult, TableInput, TableRegistrationOutcome


async def register_tables(info: Any, tables: list[TableInput]) -> list[TableRegistrationOutcome]:
    """Each of ``tables`` registered by registerTable, in order, and what each one answered. A
    refusal raised by its rights check is that table's answer, as a refusal returned is."""
    from provisa.api.admin.schema_mutation import Mutation

    outcomes: list[TableRegistrationOutcome] = []
    for table in tables:
        try:
            result = await Mutation().register_table(info, table)  # pyright: ignore[reportCallIssue]
        except PermissionError as refused:
            result = MutationResult(success=False, message=str(refused))
        outcomes.append(TableRegistrationOutcome(table_name=table.table_name, result=result))
    return outcomes
