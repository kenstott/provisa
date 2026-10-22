# Copyright (c) 2026 Kenneth Stott
# Canary: 30355d20-cdfb-4f50-9a07-e48987c9a81b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A single-source read of a code table, DIRECT to its code (REQ-2100).

The router sends a read of one code source's table -- only that table, its filters and its
parameters -- to the source's own driver, as it sends any single-source read to its source's
(``transpiler.router.decide_route``). No engine runs it and nothing is landed: the table's
function is called in its worker with the read's arguments (its ``_nf_`` conditions, and the
values its pushed predicates give, provisa/codehost/residual.py ``pushed``), and the rest of the
statement is applied to the rows by DuckDB (``residual.answer``).

The read's ``_nf_`` conditions are taken out of the statement before it is routed
(``nf_extractor.extract_nf_args``); the statement's arguments are kept here for the driver
(:func:`defer`) when the read is a candidate for this route, and its rows are not landed. A
statement routed to the engine after all lands them then (``materialization.land_deferred``).
"""

# Requirements: REQ-2100
from __future__ import annotations

import asyncio
from contextvars import ContextVar
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult

#: The arguments of the code tables the statement being planned reads DIRECT, by table name,
#: as :func:`defer` kept them; each with the table (a ``state.tables`` row).
_DEFERRED: ContextVar[dict[str, tuple[dict, dict | None]] | None] = ContextVar(
    "code_direct_reads", default=None
)

#: The dialect the router gives a code source: its statement is DuckDB's, run over the rows.
DIALECT = "duckdb"


def begin() -> None:
    """A statement is being planned: no code table of an earlier one is held for it."""
    _DEFERRED.set({})


def defer(table_name: str, table: dict, arguments: dict | None) -> None:
    """Keep ``table``'s read under ``arguments`` for its driver, under the name the statement
    gives it."""
    held = _DEFERRED.get()
    if held is None:
        held = {}
        _DEFERRED.set(held)
    held[table_name] = (table, arguments)


def deferred() -> dict[str, tuple[dict, dict | None]]:
    """The code table reads the statement holds for the driver. Kept while the statement is
    answered: its terminal may run it again (the row after its limit, a retry)."""
    return dict(_DEFERRED.get() or {})


def take() -> dict[str, tuple[dict, dict | None]]:
    """The code table reads the statement holds, no longer held: the engine reads them landed."""
    held = deferred()
    _DEFERRED.set({})
    return held


def _bound(sql: str, params: list | None) -> str:
    """``sql`` with its positional parameters as the literals they are bound to."""
    if not params:
        return sql
    import sqlglot
    from sqlglot import exp

    tree = sqlglot.parse_one(sql, read=DIALECT)
    for node in list(tree.find_all(exp.Parameter, exp.Placeholder)):
        index = int(str(node.this.this if isinstance(node.this, exp.Expr) else node.this))
        node.replace(exp.convert(params[index - 1]))
    return tree.sql(dialect=DIALECT)


class CodeDriver(DirectDriver):  # REQ-2100
    """A code source's DIRECT driver: its function called, the statement applied to its rows."""

    def __init__(self) -> None:
        self._source_id: str | None = None

    def bind_source(self, source_id: str) -> None:
        self._source_id = source_id

    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        """Nothing to connect: the source's worker is started on its first call."""

    async def close(self) -> None:
        return None

    @property
    def is_connected(self) -> bool:
        return True

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        from provisa.api.app import state

        held = deferred()
        sql = _bound(sql, params)
        reads = [(name, table, args) for name, (table, args) in held.items()]
        if len(reads) != 1 or reads[0][1]["source_id"] != self._source_id:
            raise RuntimeError(
                f"source {self._source_id!r}: a direct read names one code table, held for it "
                f"when the statement was planned; it holds {sorted(held)}"
            )
        name, table, arguments = reads[0]
        answer = await read(state, table, name, sql, arguments)
        columns = [c.to_pylist() for c in answer.columns]
        return QueryResult(rows=list(zip(*columns)), column_names=answer.column_names)


async def read(state: Any, table: dict, name: str, sql: str, arguments: dict | None) -> Any:
    """``sql`` -- a statement over the code table ``table``, named ``name`` in it -- answered
    from its function's rows: called with ``arguments`` and the values the statement pushes to
    it, the rest applied by DuckDB. An Arrow table."""
    import pyarrow as pa

    from provisa.codehost import residual
    from provisa.codesource.arrow_rows import field_of, row_schema, schema_text
    from provisa.codesource.source import context_for, prepare
    from provisa.codesource.table import CodeTableRefused, call_arguments, export_of
    from provisa.federation.registry_view import registered_sources

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
    pushdown = {
        p.name: {"column": p.pushes.column, "op": p.pushes.op}
        for p in export.parameters
        if p.pushes is not None
    }
    # An argument the statement names itself is the one the function is given: taken out of the
    # statement before it was routed, or (a table the engine attaches, REQ-2100) left in it.
    given = {
        **residual.pushed(sql, DIALECT, name, pushdown),
        **residual.nf_given(sql, DIALECT, name),
        **(arguments or {}),
    }
    args, names = call_arguments(table, export, given)
    schema = row_schema(prepared.manifest, export)
    worker = prepared.worker()

    from provisa.codesource.worker import ABSENT

    values = {n: (None if a is ABSENT else a) for n, a in zip(names, args, strict=True)}
    fields = [field_of(p.name, p.shape, prepared.manifest) for p in export.parameters]
    argument_row = pa.Table.from_pylist(
        [{f.name: _argument(values.get(f.name), f) for f in fields}], schema=pa.schema(fields)
    )

    from provisa.codesource.arrow_rows import WHOLE_ROW

    # A row type typed by a preview: each row comes whole, and the table's registered columns
    # are what the statement names, read out of it as a landed read reads them.
    loose = schema.names == [WHOLE_ROW]

    def _run() -> Any:
        if worker.where_kind == "remote" and not loose:
            # The host's front end applies the statement to the rows itself.
            answered = [
                pa.ipc.open_stream(pa.py_buffer(ipc)).read_all()
                for ipc in worker.batches(
                    function,
                    "table",
                    args,
                    names,
                    schema=schema_text(schema),
                    residual={
                        "sql": sql,
                        "dialect": DIALECT,
                        "table": name,
                        "schema": schema_text(schema),
                        "arguments": _ipc_text(argument_row),
                    },
                )
            ]
            return pa.concat_tables(answered)
        ipcs = list(worker.batches(function, "table", args, names, schema=schema_text(schema)))
        rows = _registered(table, name, ipcs) if loose else _as_returned(ipcs, schema)
        return residual.answer(sql, DIALECT, name, residual.with_arguments(rows, argument_row))

    return await asyncio.to_thread(_run)


def _argument(value: Any, field: Any) -> Any:  # a converted argument; a pyarrow Field
    """An argument in the form its Arrow field takes it: a numeric's digits and a json value's
    text, as every host sends them (provisa/codesource/arrow_rows.py)."""
    import json

    from provisa.codesource.arrow_rows import IR_KEY
    from provisa.codesource.worker import encode

    if value is None:
        return None
    ir = (field.metadata or {}).get(IR_KEY)
    if ir == b"numeric":
        return str(value)
    if ir == b"json":
        return json.dumps(encode(value), separators=(",", ":"))
    return value


def _ipc_text(table: Any) -> str:  # a pyarrow Table
    import base64

    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as stream:
        stream.write_table(table)
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()


def _as_returned(ipcs: list[bytes], schema: Any) -> Any:  # a pyarrow Schema
    """The function's rows as its host sent them, typed by its row type."""
    import pyarrow as pa

    batches = [
        b for ipc in ipcs for b in pa.ipc.open_stream(pa.py_buffer(ipc)).read_all().to_batches()
    ]
    return pa.Table.from_batches(batches) if batches else schema.empty_table()


#: A registered column's Arrow form, by its IR type; a numeric's digits and a json value's text,
#: as every host sends them (provisa/codesource/arrow_rows.py).
def _arrow_field(name: str, data_type: str) -> Any:
    import pyarrow as pa

    from provisa.codesource.arrow_rows import IR_KEY

    kinds = {
        "text": pa.utf8(),
        "bigint": pa.int64(),
        "integer": pa.int64(),
        "double": pa.float64(),
        "real": pa.float64(),
        "boolean": pa.bool_(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
    }
    if data_type in kinds:
        return pa.field(name, kinds[data_type])
    if data_type in ("numeric", "json"):
        return pa.field(name, pa.utf8(), metadata={IR_KEY: data_type.encode()})
    raise TypeError(f"column {name!r}: the IR type {data_type!r} has no Arrow form")


def _registered(table: dict, name: str, ipcs: list[bytes]) -> Any:
    """Whole rows read out into ``table``'s registered columns (``pipeline_rows.project``)."""
    import json

    import pyarrow as pa

    from provisa.codesource.arrow_rows import rows_of
    from provisa.codesource.worker import encode
    from provisa.mongodb.pipeline_rows import OutputColumn, project

    outputs = [
        OutputColumn(c["column_name"], c["data_type"], c.get("path"))
        for c in table["columns"]
        if c["native_filter_type"] is None
    ]
    fields = [_arrow_field(c.name, c.data_type) for c in outputs]

    def _cell(value: Any, data_type: str) -> Any:
        if value is None:
            return None
        if data_type == "numeric":
            return str(value)
        if data_type == "json":
            return json.dumps(encode(value), separators=(",", ":"))
        return value

    rows = [
        {c.name: _cell(row[c.name], c.data_type) for c in outputs}
        for ipc in ipcs
        for whole in rows_of(ipc)
        for row in [project(name, whole, outputs)]
    ]
    return pa.Table.from_pylist(rows, schema=pa.schema(fields))
