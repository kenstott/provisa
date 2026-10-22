# Copyright (c) 2026 Kenneth Stott
# Canary: 20cf0fe8-d299-49b4-bd42-fa5fd36cfa83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's ``@columns`` function, a batch column transform (REQ-2131).

The batched form of REQ-1924's column transform: a batch of rows in, one result per row out, in
order. Each of the function's parameters is one input column, a list of the batch's values; each
result is an object whose fields are the output columns. It is registered as a command of kind
``code_columns``, one at a time like every command, and applied to a projection's columns
(:mod:`provisa.codesource.columns_fold`).

It is stateless, so a statement's rows are cut into batches of :data:`BATCH` and the batches run
in parallel across the source's workers (``workers`` on the source): adding workers raises
throughput. What the function calls of Provisa's services -- its LLM service included (REQ-2105)
-- is metered and capped against the source as any of its calls is. A batch answered with a
different number of results than rows it was given is refused naming the function.
"""

# Requirements: REQ-2131

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

#: The implementation kind a registered ``@columns`` function's command carries (REQ-885).
COLUMN_TRANSFORM = "code_columns"
#: The schema a code source's ``@columns`` functions are offered and registered under.
SCHEMA = "code"
#: Rows a call carries: the table row batch a code source's rows already travel in.
BATCH = 1000


class ColumnTransformRefused(ValueError):
    """A ``@columns`` function that cannot be registered or applied, said naming the function."""

    code = "query.column_transform_refused"

    def __init__(self, function: str, reason: str) -> None:
        super().__init__(f"column transform {function!r}: {reason}")
        self.params = {"function": function, "reason": reason}


def _fields(manifest: Any, export: Any) -> tuple:
    from provisa.codesource.manifest import Anonymous, Named

    shape = export.returns
    if isinstance(shape, Named):
        return manifest.types[shape.name].fields
    assert isinstance(shape, Anonymous)  # the manifest admits no other @columns result
    return shape.fields


def contract(manifest: Any, export: Any) -> tuple[list[dict], list[dict]]:
    """``export``'s input columns as a command's arguments, and its output columns as its output
    contract, each ``{name, type}`` in the IR's types."""
    from provisa.codesource.discovery import data_type

    arguments = [{"name": p.name, "type": data_type(p.shape.element)} for p in export.parameters]
    outputs = [{"name": f.name, "type": data_type(f.shape)} for f in _fields(manifest, export)]
    return arguments, outputs


def _columns_export(manifest: Any, function: str) -> Any:
    return next((e for e in manifest.exports if e.name == function and e.kind == "columns"), None)


async def _prepared(state: Any, source_id: str) -> Any:  # the app state
    from provisa.codesource.source import context_for, prepare
    from provisa.federation.registry_view import registered_sources

    found = [s for s in await registered_sources(state) if s.id == source_id]
    if not found:
        raise ColumnTransformRefused(source_id, f"source {source_id!r} is not registered")
    source = found[0]
    return await prepare(source.id, source.type.value, source.mapping, await context_for(state))


async def offered(state: Any, source_id: str) -> list[tuple[str, str]]:
    """The source's ``@columns`` functions, each ``(name, what it takes and gives)``."""
    prepared = await _prepared(state, source_id)
    out = []
    for export in prepared.manifest.exports:
        if export.kind != "columns":
            continue
        arguments, outputs = contract(prepared.manifest, export)
        said = (
            f"{', '.join(a['name'] for a in arguments)} -> {', '.join(o['name'] for o in outputs)}"
        )
        out.append((export.name, " — ".join(p for p in (said, export.description) if p)))
    return out


async def registration(state: Any, source_id: str, function: str) -> tuple[list[dict], list[dict]]:
    """The arguments and output columns a command registering ``function`` carries: what the
    function declares, not what a form sent. A function that is not one of the source's
    ``@columns`` functions is refused naming it."""
    prepared = await _prepared(state, source_id)
    export = _columns_export(prepared.manifest, function)
    if export is None:
        raise ColumnTransformRefused(
            function, f"source {source_id!r} has no @columns function {function!r}"
        )
    return contract(prepared.manifest, export)


def _batches(inputs: Sequence[list], rows: int) -> list[list[list]]:
    """The input columns cut into batches of :data:`BATCH` rows, each a list of input columns."""
    return [[column[at : at + BATCH] for column in inputs] for at in range(0, rows, BATCH)]


async def transform(state: Any, command: dict, inputs: Sequence[list]) -> list[dict]:
    """The registered column transform ``command`` (a ``state.column_transforms`` entry) over the
    input columns ``inputs`` -- each a list of the statement's values, in row order -- as one
    result per row, in order: each a dict of its output columns. The batches run in parallel
    across the source's workers; a batch answered with a different number of results than rows
    it was given is refused naming the function."""
    from provisa.codesource.arrow_rows import row_schema, schema_text

    function = command["function_name"]
    rows = len(inputs[0]) if inputs else 0
    if rows == 0:
        return []
    prepared = await _prepared(state, command["source_id"])
    export = _columns_export(prepared.manifest, function)
    if export is None:
        raise ColumnTransformRefused(
            function,
            f"source {command['source_id']!r} no longer has a @columns function of that name",
        )
    schema = schema_text(row_schema(prepared.manifest, export))
    names = [p.name for p in export.parameters]
    batches = _batches(inputs, rows)
    results: list[list[dict] | None] = [None] * len(batches)

    def _run_slot(slot: int) -> None:
        # One worker's share: every ``workers``-th batch, in turn, so each worker runs one call
        # at a time and the workers run side by side.
        worker = prepared.worker(slot)
        for at in range(slot, len(batches), prepared.workers):
            batch = batches[at]
            answered = worker.columns(function, batch, names, schema=schema)
            given = len(batch[0])
            if len(answered) != given:
                raise ColumnTransformRefused(
                    function, f"it returned {len(answered)} results for a batch of {given} rows"
                )
            results[at] = answered

    await asyncio.gather(
        *(asyncio.to_thread(_run_slot, s) for s in range(min(prepared.workers, len(batches))))
    )
    return [row for batch in results if batch is not None for row in batch]
