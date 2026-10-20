# Copyright (c) 2026 Kenneth Stott
# Canary: 3342e160-fa05-4365-8c76-8b34adcb2922
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read of a MongoDB pipeline table (REQ-1980): the statement's arguments converted to their
declared types, bound into the pipeline, the pipeline run by the native reader (the only reader
that runs pipelines, whatever the federation engine), and each returned document checked against
the table's output columns. The caller lands the rows for the engine to read.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from provisa.mongodb.native_query import QUERY_ARG

# Documents fetched per round trip of the aggregate cursor.
_BATCH_ROWS = 1000


@dataclass(frozen=True)
class PipelineRead:
    rows: list[dict]
    columns: list[tuple[str, str]]  # (name, IR type), as the rows land


def is_pipeline_table(table: dict, source_types: dict[str, str]) -> bool:
    """Whether a registered table (a ``state.tables`` row) is a MongoDB pipeline table."""
    return bool(table.get("query_template")) and source_types.get(table["source_id"]) == "mongodb"


def _arguments(table: dict, given: dict | None) -> dict[str, tuple[Any, str]]:
    """Each declared argument with its converted value (None where the read gives none) and its
    type. A required argument left without a value was refused before this (REQ-1986)."""
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


async def read(state: Any, table: dict, given: dict | None) -> PipelineRead:
    """Run ``table``'s pipeline with the arguments ``given`` and return its checked rows."""
    from provisa.core.ir_types import to_ir
    from provisa.federation.registry_view import registered_sources
    from provisa.mongodb.fetch import MongoConnection, aggregate_row_batches
    from provisa.mongodb.native_query import bind, check
    from provisa.mongodb.pipeline_rows import OutputColumn, project

    name = table["table_name"]
    arguments = _arguments(table, given)
    query = check(name, table["query_template"], set(arguments))
    pipeline = bind(query, arguments)
    outputs = [
        OutputColumn(c["column_name"], c["data_type"], c.get("path"))
        for c in table["columns"]
        if c["native_filter_type"] is None
    ]
    found = [s for s in await registered_sources(state) if s.id == table["source_id"]]
    if not found:
        raise ValueError(f"Table {name!r}: its source {table['source_id']!r} is not registered")
    source = found[0]
    from provisa.core.secrets import resolve_secrets

    conn = MongoConnection.from_source(source)
    # Resolved here, at use, as its connection's host and credentials are (MongoConnection).
    database = resolve_secrets(source.database) if source.database else table["schema_name"]

    def _run() -> list[dict]:
        return [
            project(name, doc, outputs)
            for batch in aggregate_row_batches(
                conn, database, query.input_collection, pipeline, _BATCH_ROWS
            )
            for doc in batch
        ]

    rows = await asyncio.to_thread(_run)
    return PipelineRead(
        rows=rows,
        columns=[(c.name, to_ir(c.data_type)) for c in outputs],
    )


def whole_batches(conn: Any, database: str, table: Any, batch_rows: int):
    """A pipeline table with no arguments read whole, as a replica reads it: its checked rows in
    batches. ``table`` is the registry's view of it (columns with ``name``, ``data_type``,
    ``path``, ``native_filter_type``). A pipeline with arguments has no whole table."""
    from provisa.mongodb.fetch import aggregate_row_batches
    from provisa.mongodb.native_query import bind, check
    from provisa.mongodb.pipeline_rows import OutputColumn, project

    name = table.table_name
    if any(c.native_filter_type == QUERY_ARG for c in table.columns):
        raise ValueError(f"Table {name!r} is a function of its arguments: it has no whole table")
    query = check(name, table.query_template, set())
    outputs = [
        OutputColumn(c.name, c.data_type, c.path)
        for c in table.columns
        if c.native_filter_type is None
    ]
    for batch in aggregate_row_batches(
        conn, database, query.input_collection, bind(query, {}), batch_rows
    ):
        yield [project(name, doc, outputs) for doc in batch]
