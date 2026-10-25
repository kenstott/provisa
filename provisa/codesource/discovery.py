# Copyright (c) 2026 Kenneth Stott
# Canary: c12e127e-72ca-4f43-9d6c-c68c9edad6e1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What Register Table offers for a code source (REQ-1984, REQ-1993): its ``@table`` functions as
tables, and for each, the columns its row type declares and one ``query_arg`` parameter per
function parameter -- required by the source exactly when the function gives it no default
(REQ-1986). A row type declared loosely (``any``, ``unknown``, a record, a ``dict``) is typed by a
preview: the function is called once with no arguments, when it requires none, and its first rows
suggest the columns. Nothing is registered until a steward registers it.
"""

# Requirements: REQ-1984, REQ-1993, REQ-1986
from __future__ import annotations

import asyncio
from typing import Any

from provisa.codesource.manifest import (
    LOADER,
    Anonymous,
    EnumOf,
    Export,
    Field,
    ListOf,
    Manifest,
    Named,
    Scalar,
    Shape,
)
from provisa.codesource.table import QUERY_ARG

#: The rows a preview reads to suggest a loosely typed table's columns.
PREVIEW_ROWS = 5


def data_type(shape: Shape) -> str:
    """The IR type a column of ``shape`` is registered with: a scalar's own, an enum's text, and
    a list or an object as json (named object types for every source are REQ-1987's)."""
    if isinstance(shape, Scalar):
        return shape.ir
    if isinstance(shape, EnumOf):
        return "text"
    if isinstance(shape, (ListOf, Named, Anonymous)):
        return "json"
    raise TypeError(f"no column type for {shape!r}")


def _row_fields(manifest: Manifest, export: Export) -> tuple[Field, ...]:
    shape = export.returns
    if isinstance(shape, Named):
        return manifest.types[shape.name].fields
    if isinstance(shape, Anonymous):
        return shape.fields
    return ()


def _parameter_columns(export: Export) -> list[dict]:
    return [
        {
            "name": f"_nf_{p.name}",
            "data_type": data_type(p.shape),
            "path": None,
            "native_filter_type": QUERY_ARG,
            "native_filter_required": p.required,
            "comment": None,
        }
        for p in export.parameters
        # REQ-2100: a join's keys reach the loader; a statement gives it no argument.
        if not (export.loader and p.name == LOADER)
    ]


def declared_columns(manifest: Manifest, export: Export) -> list[dict]:
    """The columns ``export``'s row type declares, each described as its field is documented,
    its parameters after them."""
    data = [
        {
            "name": f.name,
            "data_type": data_type(f.shape),
            "path": None,
            "native_filter_type": None,
            "native_filter_required": None,
            "comment": f.description or None,
        }
        for f in _row_fields(manifest, export)
    ]
    return data + _parameter_columns(export)


async def _source(state: Any, source_id: str) -> Any:  # the app state; a registered Source
    from provisa.federation.registry_view import registered_sources

    found = [s for s in await registered_sources(state) if s.id == source_id]
    return found[0] if found else None


async def tables(state: Any, source_id: str) -> list[tuple[str, str]] | None:
    """The source's ``@table`` functions, ``(name, description)``; None when it is not there."""
    from provisa.codesource.source import context_for, prepare

    source = await _source(state, source_id)
    if source is None:
        return None
    prepared = await prepare(source.id, source.type.value, source.mapping, await context_for(state))
    return [(e.name, e.description) for e in prepared.manifest.exports if e.kind == "table"]


async def exports(state: Any, source_id: str) -> list[tuple[str, str]] | None:
    """Every function the source's code exports, ``(name, kind)``: its tables, commands and
    streams, as its manifest reads them; None when the source is not there (REQ-2098, its form)."""
    from provisa.codesource.source import context_for, prepare

    source = await _source(state, source_id)
    if source is None:
        return None
    prepared = await prepare(source.id, source.type.value, source.mapping, await context_for(state))
    return [(e.name, e.kind) for e in prepared.manifest.exports]


async def columns(state: Any, source_id: str, function: str) -> list[dict] | None:
    """The suggested columns of the table ``function`` reads: ``{name, data_type, path,
    native_filter_type, native_filter_required, comment}``, its parameters last."""
    from provisa.codesource.source import context_for, prepare
    from provisa.mongodb.pipeline_rows import infer_columns

    source = await _source(state, source_id)
    if source is None:
        return None
    prepared = await prepare(source.id, source.type.value, source.mapping, await context_for(state))
    export = next(
        (e for e in prepared.manifest.exports if e.name == function and e.kind == "table"), None
    )
    if export is None:
        return None
    if not export.preview:
        return declared_columns(prepared.manifest, export)
    if any(p.required for p in export.parameters):
        # A preview needs the function's arguments: its columns are the steward's to type.
        return _parameter_columns(export)
    from provisa.codesource.worker import ABSENT

    from provisa.codesource.arrow_rows import row_schema, schema_text

    worker = prepared.worker()
    names = [p.name for p in export.parameters]
    schema = schema_text(row_schema(prepared.manifest, export))

    def _sample() -> list[dict]:
        rows: list[dict] = []
        for batch in worker.rows(function, "table", [ABSENT] * len(names), names, schema=schema):
            rows.extend(batch)
            if len(rows) >= PREVIEW_ROWS:
                break
        return rows[:PREVIEW_ROWS]

    sample = await asyncio.to_thread(_sample)
    suggested = [
        {
            "name": c.name,
            "data_type": c.data_type,
            "path": c.path,
            "native_filter_type": None,
            "native_filter_required": None,
            "comment": None,
        }
        for c in infer_columns(sample)
    ]
    return suggested + _parameter_columns(export)
