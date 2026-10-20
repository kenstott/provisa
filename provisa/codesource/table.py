# Copyright (c) 2026 Kenneth Stott
# Canary: f221b57c-e01e-452e-af53-848d88cd0a3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read of a code source's table (REQ-1984, REQ-1993): one ``@table`` function of the source,
called in its worker with the statement's arguments, its rows checked against the table's columns.
The caller lands the rows for the engine under those arguments, as a pipeline table's are
(REQ-1980), and a missing required argument was refused before this (REQ-1986).

A code table is named for its function (its ``table_name`` is the function's name). Its parameters
are ``query_arg`` columns, passed to the function in its declared order; one the read gives no
value for is passed as absent, so the function's own default applies.
"""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from provisa.codesource.source import CODE_SOURCE_TYPES

QUERY_ARG = "query_arg"


class CodeTableRefused(ValueError):
    """A code table whose definition its source's code does not support, by name."""

    code = "schema.code_table_invalid"

    def __init__(self, table: str, reason: str) -> None:
        super().__init__(f"Table {table!r}: {reason}")
        self.params = {"table": table, "reason": reason}


def is_code_table(table: dict, source_types: dict[str, str]) -> bool:
    """Whether a registered table (a ``state.tables`` row) is a code source's."""
    return source_types.get(table["source_id"]) in CODE_SOURCE_TYPES


def _get(item: Any, key: str) -> Any:  # a Table model or a state.tables row, or its column
    return item.get(key) if isinstance(item, dict) else getattr(item, key, None)


def export_of(table: Any) -> str:  # a Table model or a state.tables row
    """The function a code table reads: its name is the table's (docs/arch/node-connector.md,
    section 2.2); a steward renames the table's field with its alias."""
    return _get(table, "table_name")


def check_table(table: Any, manifest: Any = None) -> None:  # a Table model or a row; a Manifest
    """A code table as registered: it has no query (it is the function it is named for); its parameters are ``query_arg`` only and
    every column is typed. With the source's ``manifest``, the function is one of its ``@table``
    functions and the parameters are exactly the function's: each by its name, required by the
    source exactly when the function requires it."""
    from provisa.core.parameters import parameter_name

    name = _get(table, "table_name")
    function = export_of(table)
    if _get(table, "query_template"):
        raise CodeTableRefused(name, "a code table is its function; it has no query")
    columns = list(_get(table, "columns") or [])
    kinds = {_get(c, "native_filter_type") for c in columns} - {None}
    if kinds - {QUERY_ARG}:
        raise CodeTableRefused(
            name, f"a code table's parameters are query_arg, not {sorted(kinds - {QUERY_ARG})}"
        )
    untyped = [
        _get(c, "name") or _get(c, "column_name") for c in columns if not _get(c, "data_type")
    ]
    if untyped:
        raise CodeTableRefused(name, f"the columns {untyped} have no data type")
    if manifest is None:
        return
    export = next((e for e in manifest.exports if e.name == function), None)
    if export is None or export.kind != "table":
        raise CodeTableRefused(name, f"its source's code has no @table function {function!r}")
    declared = {parameter_name(c): c for c in columns if _get(c, "native_filter_type") == QUERY_ARG}
    wanted = {p.name: p for p in export.parameters}
    if set(declared) != set(wanted):
        raise CodeTableRefused(
            name,
            f"its parameters {sorted(declared)} are not {function}'s parameters {sorted(wanted)}",
        )
    for pname, column in declared.items():
        if bool(_get(column, "native_filter_required")) != wanted[pname].required:
            raise CodeTableRefused(
                name,
                f"parameter {pname} is required by the function exactly when it has no default",
            )


def _arguments(table: dict, given: dict | None) -> dict[str, tuple[Any, str]]:
    """Each declared parameter with its converted value (None where the read gives none) and its
    type, by parameter name."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.core.parameter_values import convert
    from provisa.core.parameters import parameter_name

    by_canon = {apply_sql_name(k.lstrip("_")): v for k, v in (given or {}).items()}
    out: dict[str, tuple[Any, str]] = {}
    for col in table["columns"]:
        if col["native_filter_type"] != QUERY_ARG:
            continue
        name = parameter_name(col)
        value = by_canon.get(apply_sql_name(name.lstrip("_")))
        out[name] = (
            convert(value, col["data_type"], table=table["table_name"], parameter=name),
            col["data_type"],
        )
    return out


def cache_arguments(table: dict, given: dict | None) -> dict[str, str]:
    """The arguments a read gives, by name, as text: what its landed rows are kept under."""
    return {k: str(v) for k, (v, _) in _arguments(table, given).items() if v is not None}


def call_arguments(table: dict, export: Any, given: dict | None) -> tuple[list, list[str]]:
    """The function's arguments in its declared order, and their names: a value the read gives,
    else absent. ``export`` is the manifest's entry for the function."""
    from provisa.codesource.worker import ABSENT

    values = {name: value for name, (value, _) in _arguments(table, given).items()}
    args = [ABSENT if values.get(p.name) is None else values[p.name] for p in export.parameters]
    return args, [p.name for p in export.parameters]


async def read(state: Any, table: dict, given: dict | None) -> Any:  # the app state
    """Call ``table``'s function with the arguments ``given`` and return its checked rows."""
    return await read_each(state, table, [given])


async def read_each(state: Any, table: dict, argument_sets: Sequence[dict | None]) -> Any:
    """Call ``table``'s function once for each of ``argument_sets`` and return their checked rows
    together (REQ-2099: a join over a relationship that feeds the table's parameters calls it
    once per distinct set of the parent's values)."""
    from provisa.codesource.source import context_for, prepare
    from provisa.core.ir_types import to_ir
    from provisa.federation.registry_view import registered_sources
    from provisa.mongodb.pipeline_rows import OutputColumn, project
    from provisa.mongodb.pipeline_table import PipelineRead

    name = table["table_name"]
    found = [s for s in await registered_sources(state) if s.id == table["source_id"]]
    if not found:
        raise ValueError(f"Table {name!r}: its source {table['source_id']!r} is not registered")
    source = found[0]
    prepared = await prepare(source.id, source.type.value, source.mapping, await context_for(state))
    function = export_of(table)
    export = next(
        (e for e in prepared.manifest.exports if e.name == function and e.kind == "table"), None
    )
    if export is None:
        raise CodeTableRefused(name, f"its source's code has no @table function {function!r}")
    calls = [call_arguments(table, export, given) for given in argument_sets]
    outputs = [
        OutputColumn(c["column_name"], c["data_type"], c.get("path"))
        for c in table["columns"]
        if c["native_filter_type"] is None
    ]
    from provisa.codesource.arrow_rows import row_schema, schema_text

    worker = prepared.worker()
    schema = schema_text(row_schema(prepared.manifest, export))

    def _run() -> list[dict]:
        return [
            project(name, row, outputs)
            for args, names in calls
            for batch in worker.rows(function, "table", args, names, schema=schema)
            for row in batch
        ]

    rows = await asyncio.to_thread(_run)
    return PipelineRead(rows=rows, columns=[(c.name, to_ir(c.data_type)) for c in outputs])


async def whole_rows(state: Any, source: Any, table: Any) -> list[dict]:  # the app state;
    # a registered Source; a registered table (its model or the registry's view of it)
    """A code table with no parameters read whole, as a replica reads it: its function called
    with no arguments. A table with parameters is a function of its arguments and has no whole
    table."""
    from provisa.codesource.source import context_for, prepare
    from provisa.mongodb.pipeline_rows import OutputColumn, project

    name = _get(table, "table_name")
    columns = list(_get(table, "columns") or [])
    if any(_get(c, "native_filter_type") for c in columns):
        raise CodeTableRefused(
            name, "it is read under the arguments of each statement: it has no whole table"
        )
    prepared = await prepare(source.id, source.type.value, source.mapping, await context_for(state))
    function = export_of(table)
    export = next(
        (e for e in prepared.manifest.exports if e.name == function and e.kind == "table"), None
    )
    if export is None:
        raise CodeTableRefused(name, f"its source's code has no @table function {function!r}")
    outputs = [
        OutputColumn(
            _get(c, "column_name") or _get(c, "name"), _get(c, "data_type"), _get(c, "path")
        )
        for c in columns
    ]
    from provisa.codesource.arrow_rows import row_schema, schema_text

    worker = prepared.worker()
    schema = schema_text(row_schema(prepared.manifest, export))

    def _run() -> list[dict]:
        return [
            project(name, row, outputs)
            for batch in worker.rows(function, "table", [], [], schema=schema)
            for row in batch
        ]

    return await asyncio.to_thread(_run)


def make_code_loader() -> Any:  # an AdapterLoader (events/source_loader.py)
    """The whole-table load of a code table with no parameters (a replica's read)."""

    async def _load(source: Any, table: Any) -> list[dict]:
        from provisa.api.app import state

        return await whole_rows(state, source, table)

    return _load
