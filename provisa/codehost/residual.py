# Copyright (c) 2026 Kenneth Stott
# Canary: f775ac64-1b41-4475-b38c-f41eaf91317a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a read of a code table asks beyond its function's arguments, applied to the function's
output (REQ-2100).

A read of one code table is a statement over it. Its parameters go to the function as arguments;
everything else -- column filters, sorts, limits, projections, aggregates -- is applied here, by an
embedded DuckDB, to the rows the function returned, so a statement on any column never fails for
the code and its author implements none of it. Every column is named as the code declares it.

:func:`pushed` reads the values a statement gives the parameters a predicate is pushed to (the
function's pushdown mapping, provisa/codesource/pushdown.py): only top-level AND conjuncts of the
WHERE of the SELECT reading the table, each a comparison of a column with a literal. The pushed
predicate stays in the statement and is applied again here, so a function that ignores or loosens
a value cannot return wrong rows.

:func:`answer` runs the statement over the rows. The rows hold the function's output columns, and
one column per parameter, ``_nf_{name}``, holding the value the read gave it (null when none), so
a statement's own conditions on its parameters hold of every row.

STREAMED (REQ-2100). The rows go in as the host sends them, batch by batch, each typed as it
passes (a ``numeric``'s digits as :data:`NUMERIC`, the one type of every numeric column, a value
that does not fit refused by name, never rounded), and the answer comes out of DuckDB a batch at
a time: no read collects the function's rows or the answer whole. A statement that reads one
table twice (a self-join) scans its rows twice, so that table's rows are held for it.

This module is carried, as it is, by the front end of every built image and used by Provisa for a
dev-mode source; it imports nothing of Provisa's but duckdb_sql, carried beside it: the standard
library, sqlglot, DuckDB and Arrow.
"""

# Requirements: REQ-2100
from __future__ import annotations

import datetime
import importlib
from collections.abc import Iterable, Iterator, Mapping
from decimal import Decimal
from typing import Any

import duckdb
import pyarrow as pa
import sqlglot
from sqlglot import exp

# Beside this program in the image, where its modules are top-level; Provisa's, in its package.
duckdb_sql = importlib.import_module(f"{__package__}.duckdb_sql" if __package__ else "duckdb_sql")

#: The relation the statement's table is read as.
ROWS = "__provisa_code_rows"
IR_KEY = b"provisa.ir"
#: The type of every numeric column's digits: a value with more decimal places, or more whole
#: digits, than it holds is refused by name.
NUMERIC = pa.decimal128(38, 9)
#: How many rows each of the answer's batches holds at most.
ANSWER_ROWS = 65_536

_COMPARISONS: dict[type, str] = {
    exp.EQ: "eq",
    exp.NEQ: "ne",
    exp.GT: "gt",
    exp.GTE: "gte",
    exp.LT: "lt",
    exp.LTE: "lte",
}
#: A comparison read with its literal on the left: ``5 < score`` is ``score > 5``.
_FLIPPED = {"eq": "eq", "ne": "ne", "gt": "lt", "gte": "lte", "lt": "gt", "lte": "gte"}


class _NotLiteral(Exception):
    pass


def _value(node: exp.Expr) -> Any:
    """A literal's Python value: a string, a number, a boolean, a date or timestamp literal."""
    if isinstance(node, exp.Neg):
        inner = _value(node.this)
        if isinstance(inner, (int, float, Decimal)):
            return -inner
        raise _NotLiteral
    if isinstance(node, exp.Boolean):
        return node.this
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        text = str(node.this)
        return float(text) if any(c in text for c in ".eE") else int(text)
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.Literal) and node.this.is_string:
        kind = node.to.this
        if kind == exp.DataType.Type.DATE:
            return datetime.date.fromisoformat(node.this.this)
        if kind in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ):
            return datetime.datetime.fromisoformat(node.this.this)
        return node.this.this
    raise _NotLiteral


def conjuncts(node: exp.Expr | None) -> list[exp.Expr]:
    if node is None:
        return []
    if isinstance(node, exp.Paren):
        return conjuncts(node.this)
    if isinstance(node, exp.And):
        return conjuncts(node.left) + conjuncts(node.right)
    return [node]


def _reading(tree: exp.Expr, table: str) -> list[exp.Table]:
    return [t for t in tree.find_all(exp.Table) if t.name == table]


def predicate(node: exp.Expr, names: set[str]) -> tuple[str, str, Any] | None:
    """``(column, op, value)`` for a conjunct comparing one of the table's columns to a literal."""

    def column(side: exp.Expr) -> str | None:
        if isinstance(side, exp.Column) and side.table in names | {""}:
            return side.name
        return None

    try:
        op = _COMPARISONS.get(type(node))
        if op is not None and isinstance(node, exp.Binary):
            left, right = column(node.left), column(node.right)
            if left is not None and right is None:
                return left, op, _value(node.right)
            if right is not None and left is None:
                return right, _FLIPPED[op], _value(node.left)
            return None
        if isinstance(node, exp.In) and not node.args.get("query"):
            name = column(node.this)
            if name is not None:
                return name, "in", [_value(e) for e in node.expressions]
        if isinstance(node, exp.Like):
            name = column(node.this)
            if name is not None:
                value = _value(node.expression)
                return (name, "like", value) if isinstance(value, str) else None
    except _NotLiteral:
        return None
    return None


def pushed(
    sql: str, dialect: str, table: str, pushdown: Mapping[str, Mapping[str, str]]
) -> dict[str, Any]:
    """The values the statement ``sql`` gives the parameters a predicate on ``table``'s columns is
    pushed to (``pushdown``: parameter -> ``{column, op}``), by parameter. A parameter two
    conjuncts fill takes the first: each conjunct must hold, so either value is safe."""
    if not pushdown:
        return {}
    wanted = {(m["column"], m["op"]): p for p, m in pushdown.items()}
    tree = sqlglot.parse_one(sql, read=dialect)
    out: dict[str, Any] = {}
    for read in _reading(tree, table):
        select = read.parent_select
        if select is None or select.args.get("joins"):
            continue
        names = {read.alias_or_name, read.name}
        for conjunct in conjuncts(select.args["where"].this if select.args.get("where") else None):
            found = predicate(conjunct, names)
            if found is None:
                continue
            column, op, value = found
            parameter = wanted.get((column, op))
            if parameter is not None and parameter not in out:
                out[parameter] = value
    return out


def nf_given(sql: str, dialect: str, table: str) -> dict[str, Any]:
    """The values a statement's top-level ``_nf_{name} = literal`` conjuncts on ``table`` give
    its parameters, by name: what an engine that attached the host hands it."""
    tree = sqlglot.parse_one(sql, read=dialect)
    given: dict[str, Any] = {}
    for read in _reading(tree, table):
        select = read.parent_select
        where = select.args.get("where") if select is not None else None
        for conjunct in conjuncts(where.this if where is not None else None):
            if not isinstance(conjunct, exp.EQ):
                continue
            for column, value in ((conjunct.left, conjunct.right), (conjunct.right, conjunct.left)):
                if isinstance(column, exp.Column) and column.name.startswith("_nf_"):
                    try:
                        given.setdefault(column.name[len("_nf_") :], _value(value))
                    except _NotLiteral:
                        pass
    return given


def batches(ipcs: Iterable[bytes]) -> Iterator[pa.RecordBatch]:
    """Each batch of each Arrow IPC stream, as it comes."""
    for ipc in ipcs:
        yield from pa.ipc.open_stream(pa.py_buffer(ipc))


def _numeric(field: pa.Field) -> bool:
    return (field.metadata or {}).get(IR_KEY) == b"numeric"


def typed(rows: pa.RecordBatchReader) -> pa.RecordBatchReader:
    """``rows`` with each numeric field (exact digits on the wire) as :data:`NUMERIC`, batch by
    batch; a value that does not fit is refused by name."""
    fields = [pa.field(f.name, NUMERIC) if _numeric(f) else f for f in rows.schema]
    schema = pa.schema(fields)
    if schema == rows.schema:
        return rows

    def each() -> Iterator[pa.RecordBatch]:
        for batch in rows:
            columns = []
            for field, column in zip(rows.schema, batch.columns, strict=True):
                if not _numeric(field):
                    columns.append(column)
                    continue
                try:
                    columns.append(column.cast(NUMERIC))
                except pa.ArrowInvalid:
                    raise ValueError(
                        f"column {field.name!r} holds a numeric that does not fit decimal(38, 9) "
                        "(more than 9 decimal places or 29 whole digits): refused, never rounded"
                    ) from None
            yield pa.RecordBatch.from_arrays(columns, schema=schema)

    return pa.RecordBatchReader.from_batches(schema, each())


def with_arguments(
    rows: pa.RecordBatchReader | pa.Table, arguments: pa.Table
) -> pa.RecordBatchReader:
    """``rows`` with a column per parameter, ``_nf_{name}``: the one-row ``arguments``' value for
    it (null when the read gave none) on every row, batch by batch."""
    rows = _reader(rows)
    added = [
        pa.field(f"_nf_{field.name}", field.type, metadata=field.metadata)
        for field in arguments.schema
    ]
    schema = pa.schema([*rows.schema, *added])

    def each() -> Iterator[pa.RecordBatch]:
        for batch in rows:
            columns = list(batch.columns)
            for field, column in zip(added, arguments.columns, strict=True):
                columns.append(pa.repeat(column[0], batch.num_rows).cast(field.type))
            yield pa.RecordBatch.from_arrays(columns, schema=schema)

    return pa.RecordBatchReader.from_batches(schema, each())


def _reader(rows: pa.RecordBatchReader | pa.Table) -> pa.RecordBatchReader:
    return rows.to_reader() if isinstance(rows, pa.Table) else rows


def _kept(rows: pa.RecordBatchReader, failed: list[BaseException]) -> pa.RecordBatchReader:
    """``rows``, what fails as they are read kept in ``failed``: DuckDB, reading them, reports it
    inside an error of its own, and the answer raises it as it was."""

    def each() -> Iterator[pa.RecordBatch]:
        try:
            yield from rows
        except BaseException as exc:
            failed.append(exc)
            raise

    return pa.RecordBatchReader.from_batches(rows.schema, each())


def answer(
    sql: str, dialect: str, table: str, rows: pa.RecordBatchReader | pa.Table
) -> pa.RecordBatchReader:
    """``sql`` over ``rows``, the function's output as the statement's ``table``: every reference
    to the table reads them, under the name the statement gives it."""
    return answer_reads(sql, dialect, {table: rows})


def answer_reads(
    sql: str, dialect: str, rows: Mapping[str, pa.RecordBatchReader | pa.Table]
) -> pa.RecordBatchReader:
    """``sql`` over each table's rows (``rows``, by the statement's table names): every reference
    to a table reads its rows, under the name the statement gives it. The answer, a batch at a
    time as DuckDB makes it."""
    tree = sqlglot.parse_one(sql, read=dialect)
    relations: dict[str, pa.RecordBatchReader | pa.Table] = {}
    failed: list[BaseException] = []
    for i, (table, held) in enumerate(rows.items()):
        relation = f"{ROWS}_{i}"
        reads = _reading(tree, table)
        typed_rows = _kept(typed(_reader(held)), failed)
        # A reader is scanned once: a table the statement reads twice has its rows held for it.
        relations[relation] = typed_rows.read_all() if len(reads) > 1 else typed_rows
        for read in reads:
            alias = read.alias_or_name
            read.replace(exp.alias_(exp.to_table(relation), alias, table=True))
    for column in tree.find_all(exp.Column):
        # A column the statement qualifies by its table's schema or catalog is the table's.
        column.set("db", None)
        column.set("catalog", None)
    con = duckdb.connect()
    try:
        for relation, held in relations.items():
            con.register(relation, held)
        answered = con.execute(duckdb_sql.as_duckdb(tree)).fetch_record_batch(ANSWER_ROWS)
    except duckdb.Error:
        con.close()
        if failed:
            raise failed[0] from None
        raise
    except BaseException:
        con.close()
        raise

    def each() -> Iterator[pa.RecordBatch]:
        try:
            yield from answered
        except (duckdb.Error, pa.ArrowException):
            if failed:
                raise failed[0] from None
            raise
        finally:
            con.close()

    return pa.RecordBatchReader.from_batches(answered.schema, each())
