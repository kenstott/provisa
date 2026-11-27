# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c64-9e27-4b5d-a0c3-6d2e7b9f1a48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command's output columns as the objects a term or a classification is put on (REQ-1159,
amended 2026-11-25): the command, found by name in either registry, with its domain and the columns
its output contract declares."""

# Requirements: REQ-1159

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.api.errors import ApiError

if TYPE_CHECKING:
    from provisa.core.database import Connection


@dataclass(frozen=True)
class CommandOutput:
    name: str
    domain_id: str
    columns: tuple[str, ...]  # the declared output columns, by the names the service returns


async def command_output(conn: "Connection", name: str) -> CommandOutput:
    """The command ``name`` and its declared output columns; a command that is not registered is
    answered as missing (404)."""
    from provisa.api.data.action_governance import contract_columns
    from provisa.core.schema_org import tracked_functions, tracked_webhooks

    for registry in (tracked_functions, tracked_webhooks):
        row = (await conn.execute_core(select(registry).where(registry.c.name == name))).fetchone()
        if row is None:
            continue
        found = dict(row._mapping)
        cols = contract_columns(found) or []
        return CommandOutput(name, found["domain_id"], tuple(c["name"] for c in cols))
    raise ApiError(404, "actions.command_not_found", f"No command {name!r}", command=name)


def require_output_column(command: CommandOutput, column: str) -> None:
    """Refuse a column the command's output contract does not declare."""
    if column not in command.columns:
        raise ApiError(
            404,
            "actions.output_column_not_found",
            f"Command {command.name!r} declares no output column {column!r}",
            command=command.name,
            column=column,
        )
