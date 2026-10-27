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

:func:`read_context` reads what a statement asks of the table's rows -- its filters, order,
limit and columns -- as a function's read context is given them. Every part stays in the
statement and is applied again here, so a function that ignores or loosens one cannot return
wrong rows.

:func:`answer` runs the statement over the rows. The rows hold the function's output columns, and
one column per parameter, ``_nf_{name}``, holding the value the read gave it (null when none), so
a statement's own conditions on its parameters hold of every row.

STREAMED (REQ-2100). The rows go in as the host sends them, batch by batch, each typed as it
passes (a ``numeric``'s digits as :data:`NUMERIC`, the one type of every numeric column, a value
that does not fit refused by name, never rounded), and the answer comes out of DuckDB a batch at
a time: no read collects the function's rows or the answer whole. A statement that reads one
table twice (a self-join) scans its rows twice, so that table's rows are held for it.

BOUNDED (REQ-2100). Every statement runs on one DuckDB instance per process (:func:`configure`):
its memory, its threads and its spill to disk are the operator's settings. A statement that
spills past the cap -- DuckDB's own limit, or the end of the spill volume a built image is given
at the cap's size -- is refused naming ``codesource.duckdb_spill_cap_mb`` (:class:`SpillCapped`). Without an ORDER BY a statement's rows come in no particular order, as
SQL has it, which leaves DuckDB free to answer without holding them in order.

This module is carried, as it is, by the front end of every built image and used by Provisa for a
dev-mode source; it imports nothing of Provisa's but duckdb_sql and engine_memory, carried beside
it: the standard library, sqlglot, DuckDB and Arrow.
"""

# Requirements: REQ-2100
from __future__ import annotations

import datetime
import importlib
import threading
from collections.abc import Iterable, Iterator, Mapping
from decimal import Decimal
from typing import Any

import duckdb
import pyarrow as pa
import sqlglot
from sqlglot import exp

# Beside this program in the image, where its modules are top-level; Provisa's, in its package.
duckdb_sql = importlib.import_module(f"{__package__}.duckdb_sql" if __package__ else "duckdb_sql")
#: Every DuckDB this process opens is opened capped (REQ-2006): core's, carried beside it.
engine_memory = importlib.import_module(
    f"{__package__}.engine_memory" if __package__ else "engine_memory"
)

#: The relation the statement's table is read as.
ROWS = "__provisa_code_rows"
IR_KEY = b"provisa.ir"
#: The type of every numeric column's digits: a value with more decimal places, or more whole
#: digits, than it holds is refused by name.
NUMERIC = pa.decimal128(38, 9)
#: How many rows each of the answer's batches holds at most.
ANSWER_ROWS = 65_536
#: The operator setting that caps a statement's spill to disk, named when a statement reaches it.
SPILL_CAP_SETTING = "codesource.duckdb_spill_cap_mb"


class SpillCapped(ValueError):
    """A statement needed more disk to spill to than the operator's cap allows."""


class Unbounded(RuntimeError):
    """A statement before this process's DuckDB was given its bounds."""


_database: duckdb.DuckDBPyConnection | None = None
_bounds: tuple[int, int, int, str] | None = None
_database_lock = threading.Lock()


def configure(*, memory_mb: int, threads: int, spill_cap_mb: int, spill_dir: str) -> None:
    """This process's DuckDB bounds: the memory its statements run in, the threads they run on,
    the most they spill and where. Made on the first call; a later call with other bounds sets
    them on the running instance."""
    global _database, _bounds
    wanted = (memory_mb, threads, spill_cap_mb, spill_dir)
    with _database_lock:
        if wanted == _bounds:
            return
        if _database is None:
            _database = engine_memory.connect(
                extra={
                    "memory_limit": f"{memory_mb}MiB",
                    "threads": threads,
                    "max_temp_directory_size": f"{spill_cap_mb}MiB",
                    "temp_directory": spill_dir,
                    "preserve_insertion_order": False,
                }
            )
        else:
            _database.execute(f"SET memory_limit = '{int(memory_mb)}MiB'")
            _database.execute(f"SET threads = {int(threads)}")
            _database.execute(f"SET max_temp_directory_size = '{int(spill_cap_mb)}MiB'")
            # Where it spills is the process's own, fixed when the instance was made.
            assert _bounds is not None and spill_dir == _bounds[3], "the spill directory is fixed"
        _bounds = wanted


def bounded_database() -> duckdb.DuckDBPyConnection:
    """A DuckDB database of its own (a front end's catalog) under this host's memory and thread
    bounds, opened capped as every one is (REQ-2006)."""
    with _database_lock:
        if _bounds is None:
            raise Unbounded(
                "this code host's DuckDB has no bounds yet: Provisa gives them when it pairs "
                "with the host and with every call"
            )
        memory_mb, threads = _bounds[0], _bounds[1]
    return engine_memory.connect(extra={"memory_limit": f"{memory_mb}MiB", "threads": threads})


def _connection() -> duckdb.DuckDBPyConnection:
    """A connection of its own to the process's DuckDB, for one statement."""
    with _database_lock:
        if _database is None:
            raise Unbounded(
                "this code host's DuckDB has no bounds yet: Provisa gives them when it pairs "
                "with the host and with every call"
            )
        return _database.cursor()


#: What DuckDB says when its spill reaches the end of the disk it spills to: in a built image, the
#: spill volume the operator sizes to the cap (image_build.compose_snippet).
_NO_SPACE = "No space left on device"


def _named(exc: duckdb.Error) -> Exception:
    """DuckDB's error, or the refusal naming the spill cap when that is what it hit: its own
    temporary-directory limit, or the end of the volume it spills to."""
    said = str(exc)
    if _NO_SPACE in said or (
        isinstance(exc, duckdb.OutOfMemoryException) and "max_temp_directory_size" in said
    ):
        assert _bounds is not None  # the statement ran on the configured instance
        cap = _bounds[2]
        return SpillCapped(
            f"the statement needs more disk to spill to than {SPILL_CAP_SETTING} allows "
            f"({cap} MiB): raise it, or narrow the statement"
        )
    return exc


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


def predicate(node: exp.Expr, names: set[str], *, bare: bool = True) -> tuple[str, str, Any] | None:
    """``(column, op, value)`` for a conjunct comparing one of the table's columns to a literal: a
    column qualified by one of ``names``, or (``bare``) by none."""

    def column(side: exp.Expr) -> str | None:
        if isinstance(side, exp.Column) and (side.table in names or (bare and not side.table)):
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


def _wired(value: Any) -> Any:
    """A literal's value as the code-source wire carries it (provisa/codesource/worker.py)."""
    if isinstance(value, list):
        return [_wired(v) for v in value]
    if isinstance(value, datetime.datetime):
        return {"$date": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"$day": value.isoformat()}
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 2**53 - 1:
        return {"$bigint": str(value)}
    return value


def _aggregates(select: exp.Select) -> bool:
    """Whether ``select`` makes rows of its own from its input's: grouping, an aggregate, a
    window, DISTINCT or HAVING."""
    if select.args.get("group") or select.args.get("having") or select.args.get("distinct"):
        return True
    return any(node.find(exp.AggFunc, exp.Window) is not None for node in select.expressions)


def read_context(sql: str, dialect: str, table: str, columns: list[str]) -> dict[str, Any]:
    """REQ-2100: what the statement ``sql`` asks of ``table``'s rows (its declared ``columns``),
    as a code function's read context is given it: every part a hint, the statement applied to the
    rows the function returns in any case.

    - filters: each top-level AND conjunct of the WHERE of the SELECT reading the table that
      compares one of its columns with a literal (eq, ne, gt, gte, lt, lte, in, like);
    - order_by: that SELECT's keys, when each is one of the table's columns;
    - limit: its LIMIT, the OFFSET folded in -- only when it limits the table's own rows, no more
      than the filters and the order say (a SELECT of the table alone, with no grouping, aggregate,
      window or DISTINCT, every conjunct of its WHERE a filter or an input's condition, every key
      a column). A function that applies the limit after every filter and the order then holds
      every row the statement keeps;
    - columns: the ones the statement names (every one for a ``*``).

    A table the statement reads more than once has an empty context but its columns."""
    declared = list(columns)
    names_declared = set(declared)
    tree = sqlglot.parse_one(sql, read=dialect)
    reads = _reading(tree, table)
    empty: dict[str, Any] = {"filters": [], "order_by": [], "limit": None, "columns": declared}
    if len(reads) != 1:
        return empty
    (read,) = reads
    select = read.parent_select
    if select is None:
        return empty
    names = {read.alias_or_name, read.name}
    alone = not select.args.get("joins")

    def own(column: exp.Expr) -> str | None:
        """The table's column ``column`` names: qualified by the table, or bare in a SELECT that
        reads it alone."""
        if not isinstance(column, exp.Column) or column.name not in names_declared:
            return None
        if column.table in names or (alone and not column.table):
            return column.name
        return None

    filters: list[dict[str, Any]] = []
    whole = True  # every conjunct is a filter or an input's condition
    where = select.args.get("where")
    for conjunct in conjuncts(where.this if where is not None else None):
        found = predicate(conjunct, names, bare=alone)
        if found is not None and found[0] in names_declared:
            filters.append({"column": found[0], "op": found[1], "value": _wired(found[2])})
            continue
        referenced = list(conjunct.find_all(exp.Column))
        if referenced and all(c.name.startswith("_nf_") for c in referenced):
            continue  # an input's condition: it holds of every row the function returns
        whole = False
    order_by: list[dict[str, Any]] = []
    plain_order = True
    order = select.args.get("order")
    for ordered in order.expressions if order is not None else []:
        column = own(ordered.this)
        if column is None:
            plain_order = False
            break
        order_by.append(
            {
                "column": column,
                "descending": bool(ordered.args.get("desc")),
                "nulls_first": bool(ordered.args.get("nulls_first")),
            }
        )
    if not plain_order:
        order_by = []
    limit = None
    rows_alone = (
        alone
        and whole
        and plain_order
        and not _aggregates(select)
        and not isinstance(select.parent, (exp.Union, exp.Except, exp.Intersect))
    )
    limited = select.args.get("limit")
    if rows_alone and limited is not None:
        offset = select.args.get("offset")
        try:
            count = _value(limited.expression)
            skip = _value(offset.expression) if offset is not None else 0
        except _NotLiteral:
            count = skip = None  # a limit that is no literal: none is given
        if isinstance(count, int) and isinstance(skip, int):
            limit = count + skip
    if any(isinstance(s, exp.Star) for s in select.expressions) or any(
        isinstance(c, exp.Column) and isinstance(c.this, exp.Star) and c.table in names
        for c in select.expressions
    ):
        read_columns = declared
    else:
        named = {name for c in tree.find_all(exp.Column) if (name := own(c)) is not None}
        if not alone:
            named |= {c.name for c in tree.find_all(exp.Column) if c.table in names}
        read_columns = [c for c in declared if c in named]
    return {"filters": filters, "order_by": order_by, "limit": limit, "columns": read_columns}


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


class _Kept:
    """``rows`` as DuckDB reads them, what fails as they are read kept in ``failed``: DuckDB,
    reading them, reports it inside an error of its own, and the answer raises it as it was.

    DuckDB scans a reader through Arrow's read-ahead, which goes on pulling the rows on Arrow's
    threads after the statement has failed: :meth:`stop` ends that -- the function is read no
    further -- and returns once no pull is in progress, so no Arrow thread is left in Python (a
    host exiting with one hung, or in a built image crashed)."""

    def __init__(self, rows: pa.RecordBatchReader, failed: list[BaseException]) -> None:
        self._rows = rows
        self._failed = failed
        self._pulling = threading.Condition()
        self._stopped = False
        self._pulls = 0
        self.reader = pa.RecordBatchReader.from_batches(rows.schema, self._each())

    def _each(self) -> Iterator[pa.RecordBatch]:
        rows = iter(self._rows)
        while True:
            with self._pulling:
                if self._stopped:
                    return
                self._pulls += 1
            try:
                batch = next(rows, None)
            except BaseException as exc:
                self._failed.append(exc)
                raise
            finally:
                with self._pulling:
                    self._pulls -= 1
                    self._pulling.notify_all()
            if batch is None:
                return
            yield batch

    def stop(self) -> None:
        """Read no further; return once the pull in progress (the function's next batch) is
        done."""
        with self._pulling:
            self._stopped = True
            self._pulling.wait_for(lambda: self._pulls == 0)


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
    kept: list[_Kept] = []
    for i, (table, held) in enumerate(rows.items()):
        relation = f"{ROWS}_{i}"
        reads = _reading(tree, table)
        typed_rows = _Kept(typed(_reader(held)), failed)
        kept.append(typed_rows)
        # A reader is scanned once: a table the statement reads twice has its rows held for it.
        relations[relation] = typed_rows.reader.read_all() if len(reads) > 1 else typed_rows.reader
        for read in reads:
            alias = read.alias_or_name
            read.replace(exp.alias_(exp.to_table(relation), alias, table=True))
    for column in tree.find_all(exp.Column):
        # A column the statement qualifies by its table's schema or catalog is the table's.
        column.set("db", None)
        column.set("catalog", None)
    con = _connection()

    def done() -> None:
        for each in kept:
            each.stop()
        con.close()

    try:
        for relation, held in relations.items():
            con.register(relation, held)
        answered = con.execute(duckdb_sql.as_duckdb(tree)).to_arrow_reader(ANSWER_ROWS)
    except duckdb.Error as exc:
        done()
        if failed:
            raise failed[0] from None
        named = _named(exc)
        if named is exc:
            raise
        raise named from None
    except BaseException:
        done()
        raise

    def each() -> Iterator[pa.RecordBatch]:
        try:
            yield from answered
        except duckdb.Error as exc:
            if failed:
                raise failed[0] from None
            named = _named(exc)
            if named is exc:
                raise
            raise named from None
        except pa.ArrowException:
            if failed:
                raise failed[0] from None
            raise
        finally:
            done()

    return pa.RecordBatchReader.from_batches(answered.schema, each())
