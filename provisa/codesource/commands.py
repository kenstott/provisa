# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7a2c19-8b63-4d07-a1f5-6c3e9d20b8f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's ``@command`` or ``@query``, registered as a command (REQ-2226, REQ-2228).

What it is follows from the function's declaration, not from a form: a ``@command`` is a mutation
and a ``@query`` a query, with what that kind already means (REQ-1156, REQ-1928). A parameter
typed ``TableRef`` of a row type is a ``table_ref`` argument (REQ-885) carrying the input columns
that row type declares, and the function's result rows are its output contract, so the command is
a lineage node like any other (REQ-1160).

A command may instead be bound to a source's ``@command`` with some of its arguments fixed
(REQ-2228): the binding names the source and the command, and what it fixes the registration
decided, which a caller can neither change nor see as an argument. A dbt command on an
operator-run connector is one: the connector's ``@command`` takes the dbt invocation, and the
invocation is fixed for each command the manifest offers.
"""

# Requirements: REQ-885, REQ-2226, REQ-2228

from __future__ import annotations

import asyncio
from typing import Any

from provisa.api.errors import ApiError
from provisa.otel_compat import MintedSession
from provisa.codesource.manifest import (
    COMMAND,
    COMMANDS,
    Anonymous,
    Export,
    Field,
    Manifest,
    Named,
    Shape,
    TableOf,
)

#: The implementation kind a registered ``@command`` or ``@query`` carries (REQ-885).
CODE_COMMAND = "code_command"
#: The schema a code source's commands are offered and registered under, as its ``@columns``.
SCHEMA = "code"
#: The one output column of a function whose result is a value and not an object.
RESULT_COLUMN = "result"
#: A table argument's value is the table's name, as a call writes it.
_TABLE_ARGUMENT_TYPE = "text"


class CodeCommandRefused(ValueError):
    """A function that cannot be registered or called as a command, said naming it."""

    code = "query.code_command_refused"

    def __init__(self, function: str, reason: str) -> None:
        super().__init__(f"code command {function!r}: {reason}")
        self.params = {"function": function, "reason": reason}


def command_kind(export: Export) -> str:
    """What the function is within Provisa: a ``@command`` a mutation, a ``@query`` a query."""
    return "mutation" if export.kind == COMMAND else "query"


def _fields_of(manifest: Manifest, shape: Shape | None) -> tuple[Field, ...] | None:
    if isinstance(shape, Named):
        return manifest.types[shape.name].fields
    if isinstance(shape, Anonymous):
        return shape.fields
    return None


def _columns(fields: tuple[Field, ...]) -> list[dict]:
    from provisa.codesource.discovery import data_type

    return [{"name": f.name, "type": data_type(f.shape)} for f in fields]


def _argument(manifest: Manifest, export: Export, index: int) -> dict:
    from provisa.codesource.discovery import data_type

    parameter = export.parameters[index]
    if not isinstance(parameter.shape, TableOf):
        return {"name": parameter.name, "type": data_type(parameter.shape)}
    fields = _fields_of(manifest, parameter.shape.row)
    assert fields is not None  # the manifest admits no other row type for a TableRef
    return {
        "name": parameter.name,
        "type": _TABLE_ARGUMENT_TYPE,
        "arg_kind": "table_ref",
        "columns": _columns(fields),
    }


def contract(manifest: Manifest, export: Export) -> tuple[list[dict], list[dict]]:
    """``export``'s arguments, and its output columns as a command's output contract: its result
    object's fields, one ``result`` column for any other value, and none when it returns nothing
    (a VOID command, REQ-2001)."""
    from provisa.codesource.discovery import data_type

    arguments = [_argument(manifest, export, i) for i in range(len(export.parameters))]
    if export.returns is None:
        return arguments, []
    fields = _fields_of(manifest, export.returns)
    if fields is not None:
        return arguments, _columns(fields)
    return arguments, [{"name": RESULT_COLUMN, "type": data_type(export.returns)}]


def _export(manifest: Manifest, function: str) -> Export | None:
    return next((e for e in manifest.exports if e.name == function and e.kind in COMMANDS), None)


async def _prepared(state: Any, source_id: str, function: str) -> Any:  # the app state
    from provisa.codesource.source import context_for, prepare
    from provisa.federation.registry_view import registered_sources

    found = [s for s in await registered_sources(state) if s.id == source_id]
    if not found:
        raise CodeCommandRefused(function, f"source {source_id!r} is not registered")
    source = found[0]
    return await prepare(source.id, source.type.value, source.mapping, await context_for(state))


async def offered(state: Any, source_id: str) -> list[tuple[str, str]]:
    """The source's ``@command`` and ``@query`` functions, each ``(name, what it is)``."""
    prepared = await _prepared(state, source_id, source_id)
    return [
        (e.name, " — ".join(p for p in (command_kind(e), e.description) if p))
        for e in prepared.manifest.exports
        if e.kind in COMMANDS
    ]


async def registration(
    state: Any, source_id: str, function: str
) -> tuple[str, list[dict], list[dict]] | None:
    """The kind, arguments and output columns a command registering ``function`` carries; None
    when the source has no ``@command`` or ``@query`` of that name."""
    prepared = await _prepared(state, source_id, function)
    export = _export(prepared.manifest, function)
    if export is None:
        return None
    arguments, outputs = contract(prepared.manifest, export)
    return command_kind(export), arguments, outputs


def _rows(value: Any, export: Export, manifest: Manifest) -> list[dict]:
    """A call's result as rows: the rows it returned, its one object, or its value under
    ``result``; no rows when it returned nothing."""
    if export.returns is None or value is None:
        return []
    is_object = _fields_of(manifest, export.returns) is not None
    values = value if export.many else [value]
    return list(values) if is_object else [{RESULT_COLUMN: v} for v in values]


async def run(state: Any, command: dict, payload: dict, session: MintedSession) -> list[dict]:
    """Call the registered function with ``payload``, its arguments by name; one it leaves out
    takes the function's own default. A table argument is readable for the call (REQ-2226)."""
    from provisa.codesource.arrow_rows import row_schema, schema_text
    from provisa.codesource.table_reads import readable
    from provisa.codesource.worker import ABSENT

    function = command["function_name"]
    prepared = await _prepared(state, command["source_id"], function)
    export = _export(prepared.manifest, function)
    if export is None:
        raise CodeCommandRefused(
            function,
            f"source {command['source_id']!r} no longer has a @command or @query of that name",
        )
    names = [p.name for p in export.parameters]
    rows_are_objects = export.many and _fields_of(prepared.manifest, export.returns) is not None
    schema = schema_text(row_schema(prepared.manifest, export)) if rows_are_objects else None
    async with readable(state, session, payload, remote=prepared.remote is not None):
        args = [payload.get(n, ABSENT) for n in names]
        value = await asyncio.to_thread(
            prepared.worker().command, function, args, names, schema=schema
        )
    return _rows(value, export, prepared.manifest)


def _answer_rows(answer: Any) -> list[dict]:
    """A bound command's answer as rows: nothing is none, an object one row, a list of objects
    its rows, and anything else one row holding it, as a script's plain-text answer is."""
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
    """Call ``source_id``'s ``@command`` ``command`` with ``fixed`` and the caller's ``given``
    (REQ-2228)."""
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
    prepared = await _prepared(state, source_id, command)
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
    return _answer_rows(
        await asyncio.to_thread(worker.command, command, args, names, schema=schema)
    )
