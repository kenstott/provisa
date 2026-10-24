# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A single-source read of a code source's tables, DIRECT to its code (REQ-2100).

The router sends a statement that reads one code source's tables, and nothing else, to the
source's own driver, as it sends any single-source read to its source's (its proxy plans the read
as its complete dataset, codesource.proxy). No engine runs it and nothing is landed: each table's
function is called with its read's arguments, and DuckDB applies the governed statement to the
rows (provisa/codehost/residual.py). A built source's image answers a read of one of its tables
itself; a dev-mode source's rows, and those of a statement joining several tables, are answered in
Provisa's process.

The read's arguments are the statement's own: a code table read DIRECT keeps its parameters'
``_nf_{name} = value`` conditions in the statement (:func:`parameters`, as a table an engine
attaches keeps them), which give the function its arguments (``residual.nf_given``) and hold of its
rows. A statement routed to the engine after all -- it failed a gate -- has its tables landed then,
under those arguments (``materialization.land_direct_reads``).
"""

# Requirements: REQ-2100
from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult

#: The dialect the router gives a code source: its statement is DuckDB's, run over the rows.
DIALECT = "duckdb"


def direct_tables(state: Any, table_ids: Any, sql: str) -> list[dict]:
    """The tables (``state.tables`` rows) of the statement ``sql`` reads DIRECT from their code:
    every table it reads is one code source's, each read once and answered live (not floored,
    kept for good or given a landing TTL, ``attach.answered_live``). Empty for any other statement."""
    import sqlglot
    from sqlglot import exp

    from provisa.codesource.attach import answered_live
    from provisa.codesource.source import CODE_SOURCE_TYPES

    ids = set(table_ids)
    tables = [t for t in getattr(state, "tables", None) or [] if t.get("id") in ids]
    if len(tables) != len(ids):
        return []  # a table the statement reads that is no registered row: not the code's alone
    types = getattr(state, "source_types", None) or {}
    sources = {t["source_id"] for t in tables}
    if len(sources) != 1 or types.get(next(iter(sources))) not in CODE_SOURCE_TYPES:
        return []
    if not all(answered_live(state, t) for t in tables):
        return []
    reads = Counter(t.name for t in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table))
    # Every relation it reads is one of these tables, and each is read once: a table read twice
    # may be read under two argument sets, which one row set cannot hold.
    if set(reads) != {t["table_name"] for t in tables} or any(v != 1 for v in reads.values()):
        return []
    return tables


def parameters(state: Any, table_ids: Any, sql: str) -> set[str]:
    """The parameter columns (``_nf_{name}``) of the tables ``sql`` reads DIRECT: their conditions
    stay in the statement, whose arguments they are."""
    from provisa.core.parameters import NF_PREFIX, is_parameter

    return {
        c["column_name"]
        for t in direct_tables(state, table_ids, sql)
        for c in t["columns"]
        if is_parameter(c) and c["column_name"].startswith(NF_PREFIX)
    }


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
    """A code source's DIRECT driver: its tables' functions called, the statement applied to
    their rows."""

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

        assert self._source_id is not None  # bound when the pool was built (SourcePool.add)
        answer = await read(state, self._source_id, _bound(sql, params))
        columns = [c.to_pylist() for c in answer.columns]
        return QueryResult(rows=list(zip(*columns)), column_names=answer.column_names)


async def read(state: Any, source_id: str, sql: str) -> Any:
    """``sql`` -- a statement in DuckDB's dialect over tables of the code source ``source_id`` --
    answered from their functions' rows, each called with the arguments the statement gives it,
    the rest applied by DuckDB. An Arrow table."""
    import sqlglot
    from sqlglot import exp

    from provisa.codehost import residual
    from provisa.codesource.source import context_for, prepare
    from provisa.federation.registry_view import registered_sources

    found = [s for s in await registered_sources(state) if s.id == source_id]
    if not found:
        raise ValueError(f"source {source_id!r} is not registered")
    source = found[0]
    named = {t.name for t in sqlglot.parse_one(sql, read=DIALECT).find_all(exp.Table)}
    tables = [
        t
        for t in getattr(state, "tables", None) or []
        if t["source_id"] == source_id and t["table_name"] in named
    ]
    if not tables:
        raise ValueError(f"source {source_id!r}: the statement reads none of its tables")
    prepared = await prepare(source.id, source.type.value, source.mapping, await context_for(state))
    reads = [_Read.of(prepared, table, sql) for table in tables]

    def _run() -> Any:
        if len(reads) == 1 and reads[0].answered_by_image(prepared):
            return reads[0].answer_in_image(prepared, sql)
        rows = {r.name: r.rows(prepared) for r in reads}
        return residual.answer_reads(sql, DIALECT, rows)

    return await asyncio.to_thread(_run)


class _Read:
    """One table of a direct statement: its function, the arguments the statement gives it, and
    the schema of its rows."""

    def __init__(self, table: dict, export: Any, args: list, names: list, arguments: Any) -> None:
        self.table = table
        self.name = table["table_name"]
        self.export = export
        self.args = args
        self.names = names
        self.arguments = arguments  # one-row Arrow table: each parameter's value

    @classmethod
    def of(cls, prepared: Any, table: dict, sql: str) -> _Read:
        import pyarrow as pa

        from provisa.codehost import residual
        from provisa.codesource.arrow_rows import field_of
        from provisa.codesource.table import CodeTableRefused, call_arguments, export_of
        from provisa.codesource.worker import ABSENT

        name = table["table_name"]
        function = export_of(table)
        export = next(
            (e for e in prepared.manifest.exports if e.name == function and e.kind == "table"),
            None,
        )
        if export is None:
            raise CodeTableRefused(name, f"its source's code has no @table function {function!r}")
        pushdown = {
            p.name: {"column": p.pushes.column, "op": p.pushes.op}
            for p in export.parameters
            if p.pushes is not None
        }
        given = {
            **residual.pushed(sql, DIALECT, name, pushdown),
            **residual.nf_given(sql, DIALECT, name),
        }
        args, names = call_arguments(table, export, given)
        values = {n: (None if a is ABSENT else a) for n, a in zip(names, args, strict=True)}
        fields = [field_of(p.name, p.shape, prepared.manifest) for p in export.parameters]
        arguments = pa.Table.from_pylist(
            [{f.name: _argument(values.get(f.name), f) for f in fields}], schema=pa.schema(fields)
        )
        return cls(table, export, args, names, arguments)

    def _schema(self, prepared: Any) -> Any:
        from provisa.codesource.arrow_rows import row_schema

        return row_schema(prepared.manifest, self.export)

    def _loose(self, prepared: Any) -> bool:
        from provisa.codesource.arrow_rows import WHOLE_ROW

        # A row type typed by a preview: each row comes whole, and the table's registered columns
        # are what the statement names, read out of it as a landed read reads them.
        return self._schema(prepared).names == [WHOLE_ROW]

    def answered_by_image(self, prepared: Any) -> bool:
        return prepared.worker().where_kind == "remote" and not self._loose(prepared)

    def answer_in_image(self, prepared: Any, sql: str) -> Any:
        """The statement answered by the host's front end over its function's rows."""
        import pyarrow as pa

        from provisa.codesource.arrow_rows import schema_text

        schema = schema_text(self._schema(prepared))
        answered = [
            pa.ipc.open_stream(pa.py_buffer(ipc)).read_all()
            for ipc in prepared.worker().batches(
                self.export.name,
                "table",
                self.args,
                self.names,
                schema=schema,
                residual={
                    "sql": sql,
                    "dialect": DIALECT,
                    "table": self.name,
                    "schema": schema,
                    "arguments": _ipc_text(self.arguments),
                },
            )
        ]
        return pa.concat_tables(answered)

    def rows(self, prepared: Any) -> Any:
        """The function's rows, typed by its row type, each parameter's value beside them."""
        from provisa.codehost import residual
        from provisa.codesource.arrow_rows import schema_text

        schema = self._schema(prepared)
        ipcs = list(
            prepared.worker().batches(
                self.export.name, "table", self.args, self.names, schema=schema_text(schema)
            )
        )
        rows = (
            _registered(self.table, self.name, ipcs)
            if self._loose(prepared)
            else _as_returned(ipcs, schema)
        )
        return residual.with_arguments(rows, self.arguments)


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


def _arrow_field(name: str, data_type: str) -> Any:
    """A registered column's Arrow form, by its IR type; a numeric's digits and a json value's
    text, as every host sends them (provisa/codesource/arrow_rows.py)."""
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
