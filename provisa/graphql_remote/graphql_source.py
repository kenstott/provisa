# Copyright (c) 2026 Kenneth Stott
# Canary: 7a1204cb-6cc5-4f77-affe-449b18ce455d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The built-in remote GraphQL code source's entry module (REQ-2267 part 2, v0).

:func:`provisa.graphql_remote.discovered.write_code` copies this file into a code source's code
directory beside its configuration (``graphql_source.json``); it is not imported from here. It runs
under Provisa's own Python, so the mapping and reads it serves with are Provisa's
(provisa/graphql_remote/discovered.py), never a second implementation.

``discover`` generates the tables, queries and commands; one handler per kind serves them, called
with the item's name (provisa_services.Discovered).
"""

# Requirements: REQ-2267
from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, create_model

from provisa_services import (
    Discovered,
    DiscoveredCommand,
    DiscoveredQuery,
    DiscoveredRelationship,
    DiscoveredTable,
    ReadContext,
)

__all__ = ["discover", "serve_command", "serve_query", "serve_table"]


@cache
def _plan() -> Any:  # a provisa.graphql_remote.discovered.Plan
    from provisa.graphql_remote.discovered import load_config, plan

    return plan(load_config(Path(__file__).parent))


def _field(python: Any, description: str | None, alias: str, required: bool) -> tuple:
    """A model field named ``alias`` on the wire; its Python name is the model's own, so a column
    named as a Python keyword is still one."""
    if required:
        return (python, Field(..., alias=alias, description=description))
    return (python | None, Field(None, alias=alias, description=description))


def _model(name: str, fields: dict[str, Any]) -> type[BaseModel]:
    return create_model(name, **fields)


def _row(table: Any) -> type[BaseModel]:
    from provisa.graphql_remote.discovered import PYTHON_TYPE

    return _model(
        f"{table.name}_row",
        {
            f"f{i}": _field(PYTHON_TYPE[c.type], c.description, c.name, required=False)
            for i, c in enumerate(table.columns)
        },
    )


def _arguments(name: str, arguments: Any, key: Any = None) -> type[BaseModel] | None:
    from provisa.graphql_remote.discovered import PYTHON_TYPE

    fields = {
        f"f{i}": _field(PYTHON_TYPE[a.type], None, a.name, a.required)
        for i, a in enumerate(arguments)
    }
    if key is not None:
        fields[f"f{len(fields)}"] = _field(PYTHON_TYPE[key.type], None, key.name, required=False)
    return _model(name, fields) if fields else None


def _returns(command: Any) -> Any:
    """What a command that answers with one value returns: an object as a record, else its
    scalar's type."""
    from provisa.graphql_remote.discovered import PYTHON_TYPE

    if command.returns == "jsonb":
        return dict
    return PYTHON_TYPE[command.returns] | None


def discover() -> Discovered:
    """The remote's tables, queries and commands, as its schema and the mapping give them.

    @discover
    """
    plan = _plan()
    tables = []
    for t in plan.tables.values():
        key = next((c for c in t.columns if c.name == t.key), None)
        tables.append(
            DiscoveredTable(
                t.name,
                row=_row(t),
                inputs=_arguments(f"{t.name}_inputs", t.read.arguments, key),
                description=t.description or "",
                loader_batch=plan.config["loader_batch"] if key is not None else None,
            )
        )
    queries = [
        DiscoveredQuery(
            q.name,
            table=q.table,
            parameters=_arguments(f"{q.name}_parameters", q.read.arguments),
            description=q.description or "",
        )
        for q in plan.queries.values()
    ]
    commands = [
        DiscoveredCommand(
            c.name,
            parameters=_arguments(f"{c.name}_parameters", c.arguments),
            returns=None if c.table is not None else _returns(c),
            table=c.table,
            description=c.description or "",
        )
        for c in plan.commands.values()
    ]
    # The relationships the mapping detects between the tables (REQ-313), each named as the
    # graphql_remote registration names it.
    relationships = [
        DiscoveredRelationship(
            r["id"],
            source_table=r["source_table_id"],
            source_column=r["source_column"],
            target_table=r["target_table_id"],
            target_column=r["target_column"],
            cardinality=r["cardinality"],
        )
        for r in plan.relationships
    ]
    return Discovered(
        tables=tables,
        queries=queries,
        commands=commands,
        relationships=relationships,
        table_handler=serve_table,
        query_handler=serve_query,
        command_handler=serve_command,
    )


async def serve_table(name: str, inputs: dict, context: ReadContext) -> Any:
    from provisa.graphql_remote.discovered import read_table

    return await read_table(_plan(), name, inputs, context.keys)


async def serve_query(name: str, arguments: dict) -> list[dict]:
    from provisa.graphql_remote.discovered import run_query

    return await run_query(_plan(), name, arguments)


async def serve_command(name: str, arguments: dict) -> Any:
    from provisa.graphql_remote.discovered import run_command

    return await run_command(_plan(), name, arguments)
