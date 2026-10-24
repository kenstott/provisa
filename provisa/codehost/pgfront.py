# Copyright (c) 2026 Kenneth Stott
# Canary: 19eb3146-f7f1-4d81-af3a-27a5aec01b0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The front end's PostgreSQL face (REQ-2100): a federation engine attaches a built host as a
Postgres catalog and reads each ``@table`` function as a table of schema ``public``.

The front end (front.py) answers a client's SSLRequest on its one port, takes TLS with the paired
Provisa's certificate only, and hands the connection here, to buenavista's protocol handler (the
base of provisa/pgwire) with a session of the front's own:

- CATALOG: an in-memory DuckDB holds one empty table per function, its columns the code's declared
  names, exactly, and a ``_nf_{name}`` column per parameter, typed from the name tables. A statement
  that reads no function -- an engine's metadata queries over ``pg_catalog`` and
  ``information_schema`` -- is answered by DuckDB's own. ``pg_class.reltuples`` reports each
  function's row count as last answered, and -1 (unknown) before its first call, so the engine's
  planner has statistics.
- DATA: a statement that reads a function is split in two. Its parameters' values -- the
  top-level ``_nf_{name} = value`` conjuncts -- are the function's inputs, and what it asks of the
  rows is its read context (residual.read_context); the function is called in the worker, and the
  whole statement is applied to its rows by DuckDB (residual.answer).

An identifier is matched by DuckDB as PostgreSQL would: a quoted one exactly, an unquoted one
case-insensitively; the name tables refused at build any two names that cannot be told apart.
"""

# Requirements: REQ-2100
from __future__ import annotations

import base64
import datetime
import importlib
import itertools
import json
import re
import struct
import threading
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import duckdb
import pyarrow as pa
import sqlglot
from buenavista import postgres as bv
from buenavista.core import BVType, Connection, QueryResult, Session, SimpleQueryResult
from sqlglot import exp

# beside this program, in the image
residual = importlib.import_module("residual")
pgcatalog = importlib.import_module("pgcatalog")

SCHEMA = "public"

_ARROW_TYPES = {
    "text": pa.utf8(),
    "double": pa.float64(),
    "bigint": pa.int64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
    "numeric": pa.utf8(),
    "json": pa.utf8(),
    "list": pa.list_(pa.utf8()),
}

_DECLARE = re.compile(
    r"^\s*DECLARE\s+(?P<name>\w+)\s+(?:BINARY\s+)?(?:INSENSITIVE\s+)?(?:NO\s+)?(?:SCROLL\s+)?"
    r"CURSOR\s+(?:(?:WITH|WITHOUT)\s+HOLD\s+)?FOR\s+(?P<sql>.*)$",
    re.IGNORECASE | re.DOTALL,
)
_FETCH = re.compile(
    r"^\s*FETCH\s+(?:FORWARD\s+|NEXT\s+)?(?P<count>\d+|ALL)?\s*(?:FROM|IN)\s+(?P<name>\w+)\s*$",
    re.IGNORECASE,
)
_CLOSE = re.compile(r"^\s*CLOSE\s+(?P<name>\w+)\s*$", re.IGNORECASE)

#: What a statement that is a session's setting or transaction is answered with.
_SESSION_STATEMENT = re.compile(
    r"^\s*(SET|RESET|BEGIN|START\s+TRANSACTION|COMMIT|END|ROLLBACK|DISCARD|DEALLOCATE)\b",
    re.IGNORECASE,
)


def _bvtype(kind: pa.DataType) -> BVType:
    if pa.types.is_boolean(kind):
        return BVType.BOOL
    if pa.types.is_integer(kind):
        return BVType.BIGINT
    if pa.types.is_floating(kind):
        return BVType.FLOAT
    if pa.types.is_decimal(kind):
        return BVType.DECIMAL
    if pa.types.is_date(kind):
        return BVType.DATE
    if pa.types.is_timestamp(kind):
        return BVType.TIMESTAMPTZ if kind.tz else BVType.TIMESTAMP
    if pa.types.is_list(kind):
        return BVType.STRINGARRAY
    return BVType.TEXT


class ArrowResult(QueryResult):
    """Arrow rows as a query result, a batch at a time: each batch's columns read out whole, its
    rows sent, then the next batch read (REQ-2100)."""

    def __init__(self, rows: pa.Table | pa.RecordBatchReader) -> None:
        super().__init__()
        self.reader = rows.to_reader() if isinstance(rows, pa.Table) else rows
        self.schema = self.reader.schema
        self.count = 0

    def has_results(self) -> bool:
        return True

    def column_count(self) -> int:
        return len(self.schema)

    def column(self, index: int) -> tuple[str, BVType]:
        field = self.schema.field(index)
        return field.name, _bvtype(field.type)

    def rows(self) -> Iterator[list]:
        for batch in self.reader:
            lists = []
            for field, column in zip(self.schema, batch.columns, strict=True):
                values = column.to_pylist()
                if pa.types.is_list(field.type):
                    values = [None if v is None else [str(x) for x in v] for v in values]
                lists.append(values)
            for row in zip(*lists, strict=True):
                self.count += 1
                yield list(row)

    def close(self) -> None:
        """The rows not read: what answers them (the function's call, DuckDB) is stopped -- by
        letting go of the reader, whose generators end when the last reference to them goes."""
        self.reader.close()
        self.reader = pa.RecordBatchReader.from_batches(self.schema, iter(()))

    def status(self) -> str:
        return f"SELECT {self.count}"


class RowsResult(ArrowResult):
    """Rows already read (a cursor's fetch), in an Arrow result's columns."""

    def __init__(self, schema: pa.Schema, rows: list[list]) -> None:
        QueryResult.__init__(self)
        self.schema = schema
        self.count = 0
        self._rows = rows

    def rows(self) -> Iterator[list]:
        for row in self._rows:
            self.count += 1
            yield row

    def close(self) -> None:
        return None


class StatusResult(QueryResult):
    def __init__(self, status: str) -> None:
        super().__init__()
        self._status = status

    def has_results(self) -> bool:
        return False

    def column_count(self) -> int:
        return 0

    def status(self) -> str:
        return self._status


# -- the catalog ----------------------------------------------------------------------------------


class Catalog:
    """The functions' PostgreSQL catalog in one in-memory DuckDB (pgcatalog.py), each function a
    table of every one of ``schemas``; and the row counts last answered."""

    def __init__(self, names: dict, schemas: tuple[str, ...]) -> None:
        self.names = names
        self.schemas = schemas
        self._lock = threading.Lock()
        self.db = duckdb.connect()
        self.oids = pgcatalog.build(self.db, names, schemas)

    def answered(self, function: str, rows: int) -> None:
        with self._lock:
            for oid in self.oids[function]:
                self.db.execute(
                    "UPDATE pgc.pg_class SET reltuples = ? WHERE oid = ?", [float(rows), oid]
                )

    def query(self, sql: str, params: list | None) -> pa.Table:
        """A statement that reads no function, in PostgreSQL's dialect, answered from the
        catalog."""
        tree = pgcatalog.rewrite(pgcatalog.parse(sql), self.schemas)
        with self._lock:
            cursor = self.db.cursor()
        try:
            return cursor.execute(tree.sql(dialect="duckdb"), params or None).to_arrow_table()
        finally:
            cursor.close()


def schemas_of(params: dict[str, str]) -> tuple[str, ...]:
    """The schemas a connection names its functions' tables in: ``provisa.schemas`` (a comma
    list) as a startup parameter or set in ``options`` (``-c provisa.schemas=a,b``); else
    ``public``."""
    import shlex

    named = params.get("provisa.schemas")
    tokens = shlex.split(params.get("options") or "")
    for i, token in enumerate(tokens):
        setting = (
            tokens[i + 1] if token == "-c" and i + 1 < len(tokens) else token.removeprefix("-c")
        )
        key, sep, value = setting.partition("=")
        if sep and key.strip() == "provisa.schemas":
            named = value
    found = tuple(dict.fromkeys(s.strip() for s in (named or "").split(",") if s.strip()))
    return found or (SCHEMA,)


def split(text: str) -> list[str]:
    """A simple query's statements, at the semicolons outside its literals and comments."""
    from sqlglot.dialects.postgres import Postgres
    from sqlglot.tokens import TokenType

    out, start = [], 0
    for token in Postgres().tokenize(text):
        if token.token_type == TokenType.SEMICOLON:
            out.append(text[start : token.start])
            start = token.end + 1
    out.append(text[start:])
    return [s.strip() for s in out if s.strip()]


#: ``COPY (statement) TO STDOUT [(options)]``: how DuckDB's postgres scanner reads a table.
_COPY_OUT = re.compile(
    r"^\s*COPY\s*\((?P<sql>.*)\)\s*TO\s+STDOUT\s*(?:\((?P<options>[^)]*)\))?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_PGCOPY = b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0)


class FrontHandler(bv.BuenaVistaHandler):
    """buenavista's handler, a simple query of several statements answered statement by
    statement, as PostgreSQL answers it: the first that fails ends it; the session's catalog the
    one of the schemas its connection names (:func:`schemas_of`)."""

    def handle_startup(self, conn: Connection) -> Any:
        ctx = super().handle_startup(conn)
        if ctx is not None:
            front: Any = (
                self.server
            )  # serve()'s: the worker and its name tables beside buenavista's
            session: Any = ctx.session
            session.use(_catalog(front.worker, front.names, schemas_of(ctx.params)))
        return ctx

    def handle_query(self, ctx: Any, payload: bytes) -> None:
        statements = split(payload.decode("utf-8").rstrip("\x00"))
        if not statements:
            self.wfile.write(struct.pack("!ci", bv.ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            self.send_ready_for_query(ctx)
            return
        for sql in statements:
            copy = _COPY_OUT.match(sql)
            try:
                result = ctx.execute_sql(copy.group("sql") if copy else sql)
            except Exception as exc:  # noqa: BLE001 -- the statement's failure, sent to the client
                self.send_error(exc)
                break
            if copy is not None:
                if not re.search(r"\bbinary\b", copy.group("options") or "", re.IGNORECASE):
                    self.send_error(ValueError("COPY ... TO STDOUT is answered in binary format"))
                    break
                self._copy_out(result)
                continue
            if result.has_results():
                self.send_row_description(result)
                count = self.send_data_rows(result)
                self.send_command_complete(f"SELECT {count}\x00")
            else:
                self.send_command_complete(f"{result.status()}\x00")
        self.send_ready_for_query(ctx)

    def _copy_out(self, result: QueryResult) -> None:
        """``result`` as a binary COPY to the client: its columns in PostgreSQL's binary forms."""
        count = result.column_count()
        encoders = [bv.BVTYPE_TO_PGTYPE[result.column(i)[1]][2] for i in range(count)]
        response = struct.pack("!bh", 1, count) + struct.pack(f"!{count}h", *([1] * count))
        self.wfile.write(struct.pack("!ci", b"H", 4 + len(response)) + response)
        rows = 0
        chunk = bytearray(_PGCOPY)
        for row in result.rows():
            chunk += struct.pack("!h", count)
            for value, encode in zip(row, encoders, strict=True):
                if value is None:
                    chunk += struct.pack("!i", -1)
                else:
                    data = encode(value)
                    chunk += struct.pack("!i", len(data)) + data
            rows += 1
            if len(chunk) > 1 << 16:
                self.wfile.write(struct.pack("!ci", b"d", 4 + len(chunk)) + bytes(chunk))
                chunk.clear()
        chunk += struct.pack("!h", -1)
        self.wfile.write(struct.pack("!ci", b"d", 4 + len(chunk)) + bytes(chunk))
        self.wfile.write(struct.pack("!ci", b"c", 4))
        self.send_command_complete(f"COPY {rows}\x00")


# -- a read of a function -------------------------------------------------------------------------


def _bound(sql: str, params: list | None) -> str:
    """``sql`` with its positional parameters as the literals they are bound to."""
    if not params:
        return sql
    tree = sqlglot.parse_one(sql, read="postgres")
    for node in list(tree.find_all(exp.Parameter, exp.Placeholder)):
        index = int(str(node.this.this if isinstance(node.this, exp.Expr) else node.this))
        node.replace(exp.convert(params[index - 1]))
    return tree.sql(dialect="postgres")


def _wire(value: Any, kind: str) -> Any:
    """A literal a statement gives a parameter, as the code-source wire carries it to the code."""
    if value is None:
        return {"$absent": True}
    if kind == "bigint":
        return int(str(value))
    if kind == "double":
        return float(str(value))
    if kind == "boolean":
        return value if isinstance(value, bool) else str(value).lower() in ("true", "t", "1")
    if kind == "numeric":
        return {"$decimal": str(value)}
    if kind == "timestamp":
        text = value.isoformat() if isinstance(value, datetime.datetime) else str(value)
        return {"$date": text}
    if kind == "date":
        text = value.isoformat() if isinstance(value, datetime.date) else str(value)
        return {"$day": text}
    if kind == "list":
        return list(value)
    return value if isinstance(value, str) else str(value)


def _cell(value: Any, kind: str) -> Any:
    """A parameter's value as its ``_nf_`` column holds it."""
    if value is None:
        return None
    if kind == "numeric":
        return str(value)
    if kind == "json":
        return value if isinstance(value, str) else json.dumps(value)
    if kind == "timestamp" and not isinstance(value, datetime.datetime):
        return datetime.datetime.fromisoformat(str(value))
    if kind == "date" and not isinstance(value, datetime.date):
        return datetime.date.fromisoformat(str(value))
    if kind == "bigint":
        return int(str(value))
    if kind == "double":
        return float(str(value))
    if kind == "boolean":
        return value if isinstance(value, bool) else str(value).lower() in ("true", "t", "1")
    if kind == "text":
        return str(value)
    return value


class FrontSession(Session):
    def __init__(self, worker: Any) -> None:
        super().__init__()
        self.worker = worker
        self.catalog: Catalog
        self._transaction = False
        #: Each declared cursor's result, and its rows not yet fetched.
        self._cursors: dict[str, tuple[QueryResult, Iterator[list]]] = {}

    def use(self, catalog: Catalog) -> None:
        """The catalog of the schemas the session's connection names."""
        self.catalog = catalog

    def cursor(self) -> None:
        return None

    def close(self) -> None:
        return None

    def in_transaction(self) -> bool:
        return self._transaction

    def describe_sql(self, sql: str, params: list | None = None) -> QueryResult:
        result = self.execute_sql(sql, params)
        if not isinstance(result, ArrowResult):
            return result
        # Its columns only: what would answer its rows is stopped.
        result.close()
        return RowsResult(result.schema, [])

    def execute_sql(self, sql: str, params=None, result_fmt=None, **_kept) -> QueryResult:
        cursor = self._cursor_statement(sql, params)
        if cursor is not None:
            return cursor
        statement = _SESSION_STATEMENT.match(sql)
        if statement is not None:
            word = statement.group(1).upper()
            if word in ("BEGIN", "START TRANSACTION"):
                self._transaction = True
                return StatusResult("BEGIN")
            if word in ("COMMIT", "END", "ROLLBACK"):
                self._transaction = False
                return StatusResult("ROLLBACK" if word == "ROLLBACK" else "COMMIT")
            return StatusResult(word)
        if re.match(r"^\s*SHOW\s+", sql, re.IGNORECASE):
            name = sql.strip().rstrip(";").split()[-1]
            return SimpleQueryResult(name, _SHOWN.get(name.lower(), ""), BVType.TEXT)
        tree = sqlglot.parse_one(sql, read="postgres")
        functions = [
            t.name
            for t in tree.find_all(exp.Table)
            if t.name in self.catalog.names and t.db in ("", *self.catalog.schemas)
        ]
        if not functions:
            return ArrowResult(self.catalog.query(sql, params))
        if len(set(functions)) != 1:
            raise ValueError(
                f"a statement reads one function's table at a time; this one reads {sorted(set(functions))}"
            )
        return ArrowResult(self._read(functions[0], _bound(sql, params)))

    def _cursor_statement(self, sql: str, params: list | None) -> QueryResult | None:
        """DECLARE, FETCH and CLOSE of a cursor (postgres_fdw reads a foreign table through
        one): its statement answered once, its rows handed out as they are fetched."""
        declared = _DECLARE.match(sql)
        if declared is not None:
            result = self.execute_sql(declared.group("sql"), params)
            rows = result.rows() if isinstance(result, ArrowResult) else iter(())
            self._cursors[declared.group("name").lower()] = (result, rows)
            return StatusResult("DECLARE CURSOR")
        fetched = _FETCH.match(sql)
        if fetched is not None:
            held = self._cursors.get(fetched.group("name").lower())
            if held is None:
                raise ValueError(f"cursor {fetched.group('name')!r} does not exist")
            result, rows = held
            schema = result.schema if isinstance(result, ArrowResult) else pa.schema([])
            count = fetched.group("count")
            # FETCH alone is FETCH NEXT, one row; FETCH ALL, every row left: read as fetched.
            taken = 1 if count is None else None if count.upper() == "ALL" else int(count)
            return RowsResult(schema, list(itertools.islice(rows, taken)))
        closed = _CLOSE.match(sql)
        if closed is not None:
            name = closed.group("name").lower()
            ending = list(self._cursors) if name == "all" else [name]
            for each in ending:
                held = self._cursors.pop(each, None)
                if held is not None and isinstance(held[0], ArrowResult):
                    held[0].close()
            return StatusResult("CLOSE CURSOR")
        return None

    def _read(self, function: str, sql: str) -> pa.RecordBatchReader:
        table = self.catalog.names[function]
        # Its inputs are the statement's own conditions on its parameters' columns; what it asks
        # of the rows is the function's read context (REQ-2100).
        given = residual.nf_given(sql, "postgres", function)
        parameters = table["parameters"]
        names = [p["parameter"] for p in parameters]
        args = [_wire(given.get(p["parameter"]), p["type"]) for p in parameters]
        context = residual.read_context(
            sql, "postgres", function, [c["name"] for c in table["columns"]]
        )
        schema = pa.ipc.open_stream(pa.py_buffer(base64.b64decode(table["schema"]))).schema
        catalog = self.catalog

        def counted() -> Iterator[pa.RecordBatch]:
            # The function's rows as they come; its count kept for the planner once all came.
            count = 0
            for batch in residual.batches(
                self.worker.read(
                    function, args, names, table["schema"], takes=table["takes"], context=context
                )
            ):
                count += batch.num_rows
                yield batch
            catalog.answered(function, count)

        rows = pa.RecordBatchReader.from_batches(schema, counted())
        arguments = pa.Table.from_pylist(
            [{p["parameter"]: _cell(given.get(p["parameter"]), p["type"]) for p in parameters}],
            schema=pa.schema(
                [
                    pa.field(
                        p["parameter"],
                        _ARROW_TYPES[p["type"]],
                        metadata={b"provisa.ir": b"numeric"} if p["type"] == "numeric" else None,
                    )
                    for p in parameters
                ]
            ),
        )
        return residual.answer(sql, "postgres", function, residual.with_arguments(rows, arguments))


#: What SHOW answers, as a PostgreSQL server would.
_SHOWN = {
    "transaction_isolation": "read committed",
    "server_version": "16.0",
    "standard_conforming_strings": "on",
    "client_encoding": "UTF8",
    "timezone": "UTC",
    "datestyle": "ISO, MDY",
    "search_path": SCHEMA,
}


class FrontConnection(Connection):
    def __init__(self, worker: Any) -> None:
        super().__init__()
        self.worker = worker

    def new_session(self) -> Session:
        return FrontSession(self.worker)

    def parameters(self) -> dict[str, str]:
        return {
            "server_version": "16.0",
            "server_encoding": "UTF8",
            "client_encoding": "UTF8",
            "DateStyle": "ISO, MDY",
            "TimeZone": "UTC",
            "integer_datetimes": "on",
            "standard_conforming_strings": "on",
        }


_catalogs: dict[tuple[int, tuple[str, ...]], Catalog] = {}
_catalogs_lock = threading.Lock()


def _catalog(worker: Any, names: dict, schemas: tuple[str, ...]) -> Catalog:
    """The catalog of ``schemas``, one for every connection that names them."""
    with _catalogs_lock:
        key = (id(worker), schemas)
        if key not in _catalogs:
            _catalogs[key] = Catalog(names, schemas)
        return _catalogs[key]


def serve(conn: Any, worker: Any, names: dict) -> None:
    """Serve one PostgreSQL connection, already over TLS with the paired Provisa's certificate."""
    server = SimpleNamespace(
        conn=FrontConnection(worker),
        worker=worker,
        names=names,
        rewriter=None,
        extensions={},
        ctxts={},
        auth=None,
    )
    front: Any = server  # what buenavista's handler reads of its server, and the front's own
    FrontHandler(conn, conn.getpeername(), front)
