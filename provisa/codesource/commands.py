# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2e81f7-3ab4-4d09-b6f2-08a1d7c93e52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered command that is a code source's ``@command`` (REQ-885, REQ-2228).

The binding names the source and the command, and may fix some of its arguments: what the
registration decided the call does, which a caller can neither change nor see as an argument. A
dbt command on an operator-run connector is one: the connector's ``@command`` takes the dbt
invocation, and the invocation is fixed for each command the manifest offers.
"""

# Requirements: REQ-885, REQ-2228

from __future__ import annotations

import asyncio
from typing import Any

from provisa.api.errors import ApiError
from provisa.codesource.manifest import COMMAND


async def _prepared(state: Any, source_id: str) -> Any:
    from provisa.codesource.source import context_for, prepare
    from provisa.federation.registry_view import registered_sources

    found = [s for s in await registered_sources(state) if s.id == source_id]
    if not found:
        raise ApiError(
            503,
            "functions.source_not_connected",
            f"Source '{source_id}' not connected",
            source_id=source_id,
        )
    source = found[0]
    return await prepare(source.id, source.type.value, source.mapping, await context_for(state))


def _rows(answer: Any) -> list[dict]:
    """The command's answer as rows: nothing is none, an object one row, a list of objects its
    rows, and anything else one row holding it, as a script's plain-text answer is."""
    if answer is None:
        return []
    if isinstance(answer, dict):
        return [answer]
    if isinstance(answer, list) and all(isinstance(a, dict) for a in answer):
        return answer
    return [{"output": answer}]


def _refused_argument(source_id: str, command: str, names: list[str]) -> ApiError:
    return ApiError(
        422,
        "functions.code_command_argument_unknown",
        f"Command {command!r} of source {source_id!r} takes no argument {', '.join(names)}",
        source_id=source_id,
        command=command,
        arguments=", ".join(names),
    )


async def call_command(
    state: Any, source_id: str, command: str, fixed: dict[str, Any], given: dict[str, Any]
) -> list[dict]:
    """Call ``source_id``'s ``@command`` ``command`` with ``fixed`` and the caller's ``given``."""
    from provisa.codesource.arrow_rows import row_schema, schema_text
    from provisa.codesource.worker import ABSENT

    overridden = sorted(set(fixed) & set(given))
    if overridden:
        raise ApiError(
            422,
            "functions.fixed_argument_given",
            f"Command {command!r} fixes {', '.join(overridden)}: a call cannot give it",
            command=command,
            arguments=", ".join(overridden),
        )
    prepared = await _prepared(state, source_id)
    export = next(
        (e for e in prepared.manifest.exports if e.name == command and e.kind == COMMAND), None
    )
    if export is None:
        raise ApiError(
            422,
            "functions.code_command_unknown",
            f"Source {source_id!r} declares no @command {command!r}",
            source_id=source_id,
            command=command,
        )
    names = [p.name for p in export.parameters]
    unknown = sorted((set(fixed) | set(given)) - set(names))
    if unknown:
        raise _refused_argument(source_id, command, unknown)
    values = {**given, **fixed}
    args = [values.get(n, ABSENT) for n in names]
    schema = schema_text(row_schema(prepared.manifest, export)) if export.many else None
    worker = prepared.worker()
    return _rows(await asyncio.to_thread(worker.command, command, args, names, schema=schema))
