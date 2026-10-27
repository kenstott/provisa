# Copyright (c) 2026 Kenneth Stott
# Canary: d865784a-d3ad-40eb-b21a-3def676ece7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Doing what a statement does to a session's temporary table (REQ-615, REQ-1926, REQ-1942).

The pipeline has governed and run the statement's own SELECT as the session's role
(:class:`provisa.compiler.temp_tables.Action`); this lands the rows in the session's schema of
the serving engine's store, through the store's own write face, or rewrites or drops the table
there. The engine reads a temporary table at that address
(:func:`provisa.federation.temp_address.address`), so every read of one is computed by the engine,
beside the governed tables of the same statement.
"""

# Requirements: REQ-615, REQ-1926, REQ-1942

from __future__ import annotations

import asyncio
import re
from typing import Any

from provisa.compiler import temp_tables
from provisa.compiler.temp_tables import Action, TempSession, TempTable
from provisa.executor.result import QueryResult


from provisa.federation.temp_address import quoted as _quoted
from provisa.federation.temp_address import schema_of as _schema


async def _store_address(state: Any, session: TempSession, name: str) -> str:
    backend = state.federation_engine.engine.backend
    catalog = await asyncio.to_thread(backend.replica_read_catalog, state)
    return _quoted(catalog, _schema(session), name)


def _system_rows(state: Any, pg_sql: str, label: str) -> tuple[list[str], list[tuple]]:
    """The rows of the platform's own statement over a temporary table: the session's own rows,
    which no role's rules apply to."""
    from provisa.federation.execution_auth import SystemAuth, mint_system_token

    engine_rt = state.federation_engine
    sql = engine_rt.transpile_physical(pg_sql)
    schema, stream = engine_rt.execute_engine_stream(
        sql,
        authorization=SystemAuth(mint_system_token(), reason=f"temp:{label}", expected_sql=sql),
    )
    try:
        rows = [tuple(r.values()) for b in stream for r in b.to_pylist()]
    finally:
        stream.close()
    return list(schema.names), rows


#: The column types a temporary table takes, by the name a statement declares, each as the type
#: the store's write face lands. A type outside these is refused, naming the column: a value is
#: never stored as another type than its own.
_DECLARED: dict[str, str] = {
    "smallint": "bigint",
    "int": "bigint",
    "integer": "bigint",
    "bigint": "bigint",
    "real": "double",
    "double": "double",
    "double precision": "double",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",  # REQ-2050
    "text": "text",
    "varchar": "text",
    "bytea": "bytea",
}
# The SERIAL types: a bigint column whose omitted value is the table's next (TempTable.serials).
_SERIAL = frozenset({"serial", "smallserial", "bigserial"})
_DECIMAL = re.compile(r"(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\Z")


def _unsupported(name: str, kind: str) -> ValueError:
    return ValueError(
        f"temporary table column {name!r}: type {kind} is not supported. A column is one of "
        f"{', '.join(sorted(set(_DECLARED) | _SERIAL))}, or DECIMAL(precision, scale)"
    )


def landed_type(name: str, declared: str) -> str:
    """The type column ``name`` is landed as for the type a statement declares for it, refusing
    one a temporary table does not take. A decimal keeps its precision and scale."""
    spelled = " ".join(declared.lower().split())
    decimal = _DECIMAL.match(spelled)
    if decimal is not None:
        return f"numeric({int(decimal[1])},{int(decimal[2])})"
    base = spelled.split("(", 1)[0].strip()
    if base in _SERIAL:
        return "bigint"
    if base in ("decimal", "numeric"):
        raise ValueError(
            f"temporary table column {name!r}: give {declared} its precision and scale, as "
            f"DECIMAL(precision, scale)"
        )
    if base not in _DECLARED:
        raise _unsupported(name, declared)
    return _DECLARED[base]


def _arrow(landed: str) -> Any:
    import pyarrow as pa

    decimal = _DECIMAL.match(landed)
    if decimal is not None:
        return pa.decimal128(int(decimal[1]), int(decimal[2]))
    return {
        "bigint": pa.int64(),
        "double": pa.float64(),
        "boolean": pa.bool_(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "text": pa.string(),
        "bytea": pa.binary(),
    }[landed]


def inferred_type(name: str, values: list[Any]) -> str:
    """The type a column created from a SELECT is landed as: the type of its values. A column
    with no value to tell a type by, or of a type a temporary table does not take, is refused by
    name -- CAST it in the SELECT."""
    import pyarrow as pa

    kind = pa.array(values).type
    if pa.types.is_null(kind):
        raise ValueError(
            f"temporary table column {name!r} has no value to take its type from: CAST it to "
            f"its type in the SELECT"
        )
    if pa.types.is_integer(kind):
        return "bigint"
    if pa.types.is_floating(kind):
        return "double"
    if pa.types.is_decimal(kind):
        return f"numeric({kind.precision},{kind.scale})"
    if pa.types.is_boolean(kind):
        return "boolean"
    if pa.types.is_date(kind):
        return "date"
    if pa.types.is_timestamp(kind):
        return "timestamp" if kind.tz is None else "timestamptz"
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return "text"
    raise _unsupported(name, str(kind))


def _bytea(value: Any) -> Any:
    """A bytea value given as text, read as PostgreSQL's bytea input reads it: ``\\x`` then hex
    digits, else the text's own bytes with ``\\\\`` for one backslash and ``\\nnn`` an octal
    byte."""
    if not isinstance(value, str):
        return value
    if value.startswith("\\x"):
        return bytes.fromhex(value[2:])
    out = bytearray()
    i = 0
    while i < len(value):
        if value[i] == "\\" and value[i + 1 : i + 2] == "\\":
            out.append(0x5C)
            i += 2
        elif value[i] == "\\" and len(value) >= i + 4 and value[i + 1 : i + 4].isdigit():
            out.append(int(value[i + 1 : i + 4], 8))
            i += 4
        elif value[i] == "\\":
            raise ValueError(f"invalid input syntax for type bytea: {value!r}")
        else:
            out.extend(value[i].encode("utf-8"))
            i += 1
    return bytes(out)


def _batch(columns: list[tuple[str, str]], rows: list[tuple]) -> Any:
    """``rows`` as one Arrow batch of ``columns``, each (name, landed type). A value that is not
    of its column's type fails here, naming the column: none is stored as another type."""
    import pyarrow as pa

    arrays = {}
    for i, (name, landed) in enumerate(columns):
        values = [r[i] for r in rows]
        if landed == "bytea":
            values = [_bytea(v) for v in values]
        try:
            arrays[name] = pa.array(values, _arrow(landed))
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise ValueError(
                f"temporary table column {name!r} is {landed}; a value given for it is not: {exc}"
            ) from exc
    return pa.RecordBatch.from_pydict(arrays)


def _within_tables(session: TempSession, name: str) -> None:
    """A new table past ``temp_tables.max_per_session`` is refused by name (REQ-1926)."""
    from provisa.core import settings_registry

    limit = int(settings_registry.value("temp_tables.max_per_session"))
    if len(session.tables) >= limit:
        raise ValueError(
            f"temporary table {name!r}: this session holds {len(session.tables)} temporary "
            f"table(s), the most one may hold (temp_tables.max_per_session = {limit}); drop one "
            f"first"
        )


def _within_rows(name: str, rows: int) -> None:
    """A table that would hold more than ``temp_tables.max_rows`` is refused, not cut (REQ-1926):
    a temporary table is a real table of the store that a role with no write right may fill."""
    from provisa.core import settings_registry

    limit = int(settings_registry.value("temp_tables.max_rows"))
    if rows > limit:
        raise ValueError(
            f"temporary table {name!r} would hold {rows} rows, more than one may "
            f"(temp_tables.max_rows = {limit})"
        )


async def _land(state: Any, session: TempSession, name: str, batch: Any, columns: list) -> None:
    from provisa.federation.data_replicator import data_replicator
    from provisa.federation.replica_address import ReplicaAddress
    from provisa.federation.replica_parties import StoreReadingEngine
    from provisa.federation.replica_source import BATCH_ROWS
    from provisa.federation.residency import LandingArgs
    from provisa.synthetic.run import _LandedRows

    backend = state.federation_engine.engine.backend
    party = StoreReadingEngine(backend, state)
    target = backend.replica_target(
        state,
        address=ReplicaAddress(_schema(session), name),
        args=LandingArgs(
            columns=columns, change_signal="ttl", watermark_column=None, pk_columns=[]
        ),
        engine=party,
    )

    async def progress(rows: int) -> None:
        del rows  # a session's own table reports nothing

    if not session.stored:
        # Recorded before anything is written, so a schema the store holds is one the keeper
        # can drop when the session is gone without ending (provisa.pgwire.temp_keeper).
        from provisa.pgwire.temp_keeper import note_stored

        await note_stored(state, session, _schema(session))
    await data_replicator(_LandedRows(batch), target, party, batch_rows=BATCH_ROWS).run(progress)
    session.stored = True


def _next_serial(table: TempTable, column: str) -> int | None:
    """The value an INSERT omitting ``column`` gives it: a SERIAL's next, else NULL."""
    if column not in table.serials:
        return None
    table.serials[column] += 1
    return table.serials[column]


async def apply(
    action: Action, result: QueryResult, state: Any, role_id: str | None
) -> QueryResult:
    """Do ``action`` to the current session's temporary table with ``result``, the rows its
    governed SELECT gave ``role_id``; the statement's own answer, a count of rows and no row."""
    session = temp_tables.current()
    assert session is not None, "a temporary-table statement is planned inside its session"
    name = action.name
    if action.kind == "drop":
        from provisa.federation.store_scope import drop_temp_table

        await drop_temp_table(
            state.federation_engine.engine.materialize_store(), _schema(session), name
        )
        del session.tables[name]
        session.generation += 1
        return QueryResult(rows=[], column_names=[], rowcount=0)
    if action.kind == "create":
        _within_tables(session, name)
        serials: dict[str, int] = {}
        if action.columns is not None:
            columns = [(c, landed_type(c, declared)) for c, declared in action.columns]
            serials = {
                c: 0
                for c, declared in action.columns
                if declared.lower().split("(", 1)[0].strip() in _SERIAL
            }
            rows: list[tuple] = []
        else:
            rows = list(result.rows)
            columns = [
                (c, inferred_type(c, [r[i] for r in rows]))
                for i, c in enumerate(result.column_names)
            ]
        _within_rows(name, len(rows))
        await _land(state, session, name, _batch(columns, rows), columns)
        session.tables[name] = TempTable(name, columns, serials)
        session.generation += 1
        return QueryResult(rows=[], column_names=[], rowcount=len(rows))
    table = session.tables[name]
    at = await _store_address(state, session, name)
    if action.kind == "insert":
        assert action.columns is not None
        named = [c for c, _ in action.columns]
        if len(result.column_names) != len(named):
            raise ValueError(
                f"INSERT INTO {name}: {len(named)} column(s) named, "
                f"{len(result.column_names)} value(s) given"
            )
        _, held = await asyncio.to_thread(_system_rows, state, f"SELECT * FROM {at}", name)
        position = {c: i for i, c in enumerate(named)}
        added = [
            tuple(
                r[position[c]] if c in position else _next_serial(table, c)
                for c, _ in table.columns
            )
            for r in result.rows
        ]
        _within_rows(name, len(held) + len(added))
        await _land(state, session, name, _batch(table.columns, [*held, *added]), table.columns)
        return QueryResult(rows=[], column_names=[], rowcount=len(added))
    assert action.kind == "rewrite" and action.rewrite is not None
    _, before = await asyncio.to_thread(_system_rows, state, f"SELECT * FROM {at}", name)
    _, after = await asyncio.to_thread(
        _system_rows, state, action.rewrite.format(table=f"{at} AS {_quoted(name)}"), name
    )
    _within_rows(name, len(after))
    await _land(state, session, name, _batch(table.columns, after), table.columns)
    # A DELETE removes the rows that are gone; an UPDATE keeps every row, changed or not.
    return QueryResult(rows=[], column_names=[], rowcount=len(before) - len(after))


async def end(session: TempSession, state: Any) -> None:
    """The session is over: its temporary tables go with it."""
    if session.stored:
        from provisa.federation.store_scope import drop_temp_schema
        from provisa.pgwire.temp_keeper import note_ended

        schema = _schema(session)
        await drop_temp_schema(state.federation_engine.engine.materialize_store(), schema)
        await note_ended(state, session, schema)
    session.tables.clear()
    session.stored = False
