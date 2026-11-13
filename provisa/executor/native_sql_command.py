# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f4f0e-6d0b-4c1e-9a43-5b2f7d8e91c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A call of a command holding native SQL for a relational source (REQ-1988, amended 2026-11-13).

The text, checked when the command was saved (:mod:`provisa.core.native_sql`), runs directly on the
source's direct connection, never through the engine or the semantic pipeline. A call converts each
argument to its declared type and binds every occurrence as a driver parameter, so a value is never
part of the text. The statements run in one native transaction: all commit or none does. The call
answers the last statement's rows. A command declared as a query runs in a READ ONLY transaction,
so the source refuses a write in its text; one declared as a mutation runs read-write. The call
is governed as a source procedure is (``api/data/action_exec.invoke_command``).

The run is written per source type: :data:`_RUNNERS` holds one entry per type whose direct driver
runs the statements, beside its dialect in ``core.native_sql.DIALECTS``.
"""

# Requirements: REQ-1988

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from provisa.api.errors import ApiError
from provisa.core.native_sql import NativeStatement, check_on_source
from provisa.security.mutation_authz import MutationKind, classify_kind


async def _run_postgresql(
    driver: Any, statements: list[NativeStatement], values: dict[str, Any], read_only: bool
) -> Any:
    """The statements on a PostgreSQL source's direct connection, in one transaction. Each
    argument is a psycopg named parameter, sent to the server as a bound value; a ``%`` of the
    text is doubled, as psycopg reads it. A ``read_only`` run is a READ ONLY transaction: the
    database refuses a write in it."""
    rendered: list[tuple[str, dict[str, Any]]] = []
    for statement in statements:
        pieces = [statement.texts[0].replace("%", "%%")]
        for argument, text in zip(statement.names, statement.texts[1:]):
            pieces.append(f"%(a_{argument})s")
            pieces.append(text.replace("%", "%%"))
        params = {f"a_{argument}": values[argument] for argument in statement.names}
        rendered.append(("".join(pieces), params))
    return await driver.execute_in_one_transaction(rendered, read_only=read_only)


# How native SQL runs on each source type: the statements in one transaction on the source's
# direct driver, answering the last statement's result.
_RUNNERS: dict[
    str, Callable[[Any, list[NativeStatement], dict[str, Any], bool], Awaitable[Any]]
] = {
    "postgresql": _run_postgresql,
}


async def run(state: Any, fn: dict, args: dict) -> list[dict]:
    """Run the native SQL command ``fn`` with ``args`` (by argument name) on its source, and
    return the last statement's rows."""
    from provisa.core.parameter_values import convert
    from provisa.executor.serialize import result_records

    name = fn["name"]
    source_id = fn["source_id"]
    source_type = state.source_types.get(source_id)
    if source_type is None or not state.source_pools.has(source_id):
        raise ApiError(
            503,
            "functions.source_not_connected",
            f"Source '{source_id}' not connected",
            source_id=source_id,
        )
    declared = fn.get("arguments") or []
    statements = check_on_source(
        name, source_id, source_type, fn["binding"], [a["name"] for a in declared]
    )
    values = {
        a["name"]: convert(args.get(a["name"]), a["type"], table=name, parameter=a["name"])
        for a in declared
    }
    # A command declared as a query reads: its transaction is READ ONLY, so the source itself
    # stops a write in its text. One declared as a mutation runs read-write.
    reads = classify_kind(fn.get("kind")) is MutationKind.READ
    result = await _RUNNERS[source_type](
        state.source_pools.get(source_id), statements, values, reads
    )
    return result_records(result)
