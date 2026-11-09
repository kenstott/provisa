# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2c4f17-9a6d-4b33-b1e8-7d05c2a9f361
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Snowflake source's Dynamic Tables and Tasks (REQ-2230).

A schema's Dynamic Tables are read with SHOW, which also gives the statement that created each
one. What can be run on them is offered object by object, so a command is one operation on one
object and a role holding it holds nothing else: refreshing, suspending and resuming a Dynamic
Table, executing, suspending and resuming a Task. An operation is named ``<verb>.<object>``.

Each statement names its object as an identifier, and only an object SHOW has just listed is run.
"""

# Requirements: REQ-2230

from __future__ import annotations

from dataclasses import dataclass

import sqlglot.expressions as exp

from provisa.executor.result import QueryResult

SNOWFLAKE = "snowflake"

_DYNAMIC_TABLE_STATEMENTS = {
    "refresh_dynamic_table": "ALTER DYNAMIC TABLE {} REFRESH",
    "suspend_dynamic_table": "ALTER DYNAMIC TABLE {} SUSPEND",
    "resume_dynamic_table": "ALTER DYNAMIC TABLE {} RESUME",
}
_TASK_STATEMENTS = {
    "execute_task": "EXECUTE TASK {}",
    "suspend_task": "ALTER TASK {} SUSPEND",
    "resume_task": "ALTER TASK {} RESUME",
}
_STATEMENTS = {**_DYNAMIC_TABLE_STATEMENTS, **_TASK_STATEMENTS}


@dataclass(frozen=True)
class DynamicTable:
    name: str
    comment: str | None
    #: The statement that created it, as Snowflake reports it.
    definition: str | None


def names_operation(function_name: str) -> bool:
    """Whether ``function_name`` is written as an operation of this module, not a routine."""
    verb, dot, _ = function_name.partition(".")
    return bool(dot) and verb in _STATEMENTS


def _schema(schema_name: str) -> str:
    return exp.to_identifier(schema_name, quoted=True).sql(dialect=SNOWFLAKE)


def _object(schema_name: str, name: str) -> str:
    return exp.Table(
        this=exp.to_identifier(name, quoted=True),
        db=exp.to_identifier(schema_name, quoted=True),
    ).sql(dialect=SNOWFLAKE)


def _statement(schema_name: str, operation: str) -> str:
    verb, _, name = operation.partition(".")
    return _STATEMENTS[verb].format(_object(schema_name, name))


async def _execute(pool, source_id: str, sql: str, operation: str) -> QueryResult:
    """Run ``sql``; a statement Snowflake refuses comes back as it stated it."""
    from snowflake.connector.errors import Error

    from provisa.executor.source_operation import _refused

    try:
        return await pool.execute(source_id, sql)
    except Error as exc:
        raise _refused(source_id, operation, None, str(exc)) from exc


async def _show(pool, source_id: str, what: str, schema_name: str) -> list[dict]:
    """Each object of a SHOW statement, its columns by lower-case name."""
    result = await _execute(
        pool, source_id, f"SHOW {what} IN SCHEMA {_schema(schema_name)}", f"SHOW {what}"
    )
    names = [c.lower() for c in result.column_names]
    return [dict(zip(names, row, strict=True)) for row in result.rows]


async def dynamic_tables(pool, source_id: str, schema_name: str) -> list[DynamicTable]:
    """The Dynamic Tables of ``schema_name``."""
    return [
        DynamicTable(name=row["name"], comment=row.get("comment") or None, definition=row["text"])
        for row in await _show(pool, source_id, "DYNAMIC TABLES", schema_name)
    ]


async def operations(pool, source_id: str, schema_name: str) -> list[tuple[str, str]]:
    """Every operation ``schema_name`` offers, as (operation name, the statement it runs)."""
    tables = await dynamic_tables(pool, source_id, schema_name)
    tasks = await _show(pool, source_id, "TASKS", schema_name)
    names = [
        *(f"{verb}.{t.name}" for t in tables for verb in _DYNAMIC_TABLE_STATEMENTS),
        *(f"{verb}.{t['name']}" for t in tasks for verb in _TASK_STATEMENTS),
    ]
    return [(name, _statement(schema_name, name)) for name in names]


async def run(pool, source_id: str, schema_name: str, operation: str) -> list[dict]:
    """Run ``operation`` on the source's connection, its answer as the rows a command returns."""
    from provisa.executor.serialize import result_records

    sql = _statement(schema_name, operation)
    return result_records(await _execute(pool, source_id, sql, operation))
