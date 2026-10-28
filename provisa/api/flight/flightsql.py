# Copyright (c) 2026 Kenneth Stott
# Canary: 278a2636-5625-4bfe-b85e-ff367571f943
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Arrow Flight SQL protocol on Provisa's Flight endpoint (REQ-2175).

A Flight SQL client sends its commands as a ``google.protobuf.Any`` wrapping a FlightSql.proto
message: the command of a FlightDescriptor (GetFlightInfo), the bytes of a Ticket (DoGet). This
module packs and unpacks them and builds the answers that are the protocol's own (the statement's
FlightInfo, SqlInfo); executing a statement is the server's governed pipeline, the one the native
JSON ticket uses.
"""

# Requirements: REQ-2175

from __future__ import annotations

import importlib.metadata
import json

import pyarrow as pa
import pyarrow.flight as flight
from google.protobuf import any_pb2
from google.protobuf.message import DecodeError, Message

from provisa.proto import Flight_pb2 as flight_pb
from provisa.proto import FlightSql_pb2 as pb

_TYPE_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql."


def unpack(raw: bytes) -> Message | None:
    """The Flight SQL message ``raw`` carries, or None when it is not a Flight SQL ``Any`` (a
    native JSON ticket or path descriptor)."""
    wrapper = any_pb2.Any()
    try:
        wrapper.ParseFromString(raw)
    except DecodeError:
        return None
    if not wrapper.type_url.startswith(_TYPE_PREFIX):
        return None
    name = wrapper.type_url[len(_TYPE_PREFIX) :]
    message_class = getattr(pb, name, None)
    if message_class is None:
        raise flight.FlightServerError(f"unknown Flight SQL message {name!r}")  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    message = message_class()
    wrapper.Unpack(message)
    return message


def pack(message: Message) -> bytes:
    """``message`` wrapped in the ``Any`` a Flight SQL client reads."""
    wrapper = any_pb2.Any()
    wrapper.Pack(message, type_url_prefix="type.googleapis.com/")
    return wrapper.SerializeToString()


def statement_ticket(query: str) -> bytes:
    """The ticket a statement query's rows are fetched with: the statement itself, which DoGet
    runs as the caller its call headers authenticate."""
    handle = json.dumps({"query": query}).encode("utf-8")
    return pack(pb.TicketStatementQuery(statement_handle=handle))


def statement_query(ticket: pb.TicketStatementQuery) -> str:
    """The statement a :func:`statement_ticket` carries."""
    return json.loads(ticket.statement_handle.decode("utf-8"))["query"]


def statement_info(
    descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    query: str,
) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A statement query's FlightInfo: one endpoint on this server, whose ticket runs it. It carries
    no schema: the statement is compiled when its ticket is run, and its rows arrive with theirs,
    which the client reads when the info names none."""
    endpoint = flight.FlightEndpoint(statement_ticket(query), [])  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return flight.FlightInfo(None, descriptor, [endpoint], -1, -1)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


# --- SqlInfo ----------------------------------------------------------------------------------

#: The schema FlightSql.proto gives CommandGetSqlInfo's answer.
SQL_INFO_SCHEMA = pa.schema(
    [
        pa.field("info_name", pa.uint32(), nullable=False),
        pa.field(
            "value",
            pa.dense_union(
                [
                    pa.field("string_value", pa.utf8()),
                    pa.field("bool_value", pa.bool_()),
                    pa.field("bigint_value", pa.int64()),
                    pa.field("int32_bitmask", pa.int32()),
                    pa.field("string_list", pa.list_(pa.utf8())),
                    pa.field("int32_to_int32_list_map", pa.map_(pa.int32(), pa.list_(pa.int32()))),
                ]
            ),
            nullable=False,
        ),
    ]
)


def _server_info() -> dict[int, str | bool | int]:
    """What Provisa answers of SqlInfo: the server's name and versions, that it takes SQL, and
    that it takes transactions -- acknowledged, holding none (REQ-883)."""
    return {
        pb.FLIGHT_SQL_SERVER_NAME: "Provisa",
        pb.FLIGHT_SQL_SERVER_VERSION: importlib.metadata.version("provisa"),
        pb.FLIGHT_SQL_SERVER_ARROW_VERSION: pa.__version__,
        pb.FLIGHT_SQL_SERVER_SQL: True,
        pb.FLIGHT_SQL_SERVER_TRANSACTION: pb.SQL_SUPPORTED_TRANSACTION_TRANSACTION,
    }


def sql_info_table(command: pb.CommandGetSqlInfo) -> pa.Table:
    """CommandGetSqlInfo's answer: each info Provisa has, of those asked (all when none is)."""
    known = _server_info()
    asked = list(command.info) or sorted(known)
    names: list[int] = []
    strings: list[str] = []
    bools: list[bool] = []
    ints: list[int] = []
    type_ids: list[int] = []
    offsets: list[int] = []
    for name in asked:
        if name not in known:
            continue
        value = known[name]
        names.append(name)
        if isinstance(value, bool):
            type_ids.append(1)
            offsets.append(len(bools))
            bools.append(value)
        elif isinstance(value, int):
            type_ids.append(3)  # int32_bitmask: an enum value, as FLIGHT_SQL_SERVER_TRANSACTION is
            offsets.append(len(ints))
            ints.append(value)
        else:
            type_ids.append(0)
            offsets.append(len(strings))
            strings.append(value)
    union_type = SQL_INFO_SCHEMA.field("value").type
    children = [
        pa.array(strings, pa.utf8()),
        pa.array(bools, pa.bool_()),
        pa.array([], pa.int64()),
        pa.array(ints, pa.int32()),
        pa.array([], pa.list_(pa.utf8())),
        pa.array([], union_type.field(5).type),
    ]
    values = pa.UnionArray.from_dense(
        pa.array(type_ids, pa.int8()),
        pa.array(offsets, pa.int32()),
        children,
        [union_type.field(i).name for i in range(union_type.num_fields)],
    )
    return pa.Table.from_arrays([pa.array(names, pa.uint32()), values], schema=SQL_INFO_SCHEMA)


def sql_info_info(
    descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """CommandGetSqlInfo's FlightInfo: its ticket is the command itself."""
    endpoint = flight.FlightEndpoint(descriptor.command, [])  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return flight.FlightInfo(SQL_INFO_SCHEMA, descriptor, [endpoint], -1, -1)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


# --- prepared statements ----------------------------------------------------------------------
#
# A prepared statement is stateless: its handle carries the statement and, once DoPut binds them,
# its parameter rows (DoPutPreparedStatementResult hands the client the new handle, which the
# protocol provides for exactly this). Nothing is kept on the server, so ClosePreparedStatement has
# nothing to release, and any worker of a deployment answers any handle.


def prepared_handle(query: str, parameters: pa.Table | None = None) -> bytes:
    """The handle of ``query`` with ``parameters`` bound: its statement as JSON on the first line,
    then the parameter rows as an Arrow IPC stream."""
    head = json.dumps({"query": query}).encode("utf-8") + b"\n"
    if parameters is None:
        return head
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, parameters.schema) as writer:
        writer.write_table(parameters)
    return head + sink.getvalue().to_pybytes()


def read_prepared_handle(handle: bytes) -> tuple[str, pa.Table | None]:
    """The statement and the bound parameter rows (None before DoPut binds any) of a
    :func:`prepared_handle`."""
    head, _, body = handle.partition(b"\n")
    try:
        query = json.loads(head.decode("utf-8"))["query"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise flight.FlightServerError("not a prepared statement handle of this server") from exc  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if not body:
        return query, None
    return query, pa.ipc.open_stream(body).read_all()


def bound_statements(query: str, parameters: pa.Table | None) -> list[str]:
    """``query`` once per parameter row, its ``$N`` placeholders written as that row's SQL
    literals, as pgwire writes a bound statement's before governing it; ``query`` itself when
    nothing is bound."""
    from provisa.compiler.params import substitute_positional_placeholders
    from provisa.compiler.sql_literals import sql_literal

    if parameters is None or parameters.num_columns == 0:
        return [query]
    names = parameters.column_names
    return [
        substitute_positional_placeholders(
            query,
            [row[name] for name in names],
            lambda i, row=row: sql_literal(row[names[i - 1]], "postgres"),
        )
        for row in parameters.to_pylist()
    ]


def bound_statement(query: str, parameters: pa.Table | None) -> str:
    """``query`` with its parameters bound (:func:`bound_statements`). Each parameter row is one
    execution; several are answered as one statement, their results in turn (UNION ALL)."""
    statements = bound_statements(query, parameters)
    if len(statements) == 1:
        return statements[0]
    return "\nUNION ALL\n".join(f"({statement})" for statement in statements)


def _temporary_type(arrow: pa.DataType) -> str:
    """The PostgreSQL type an ingested column of ``arrow`` is declared as in a temporary table."""
    if pa.types.is_boolean(arrow):
        return "boolean"
    if pa.types.is_int8(arrow) or pa.types.is_int16(arrow) or pa.types.is_uint8(arrow):
        return "smallint"
    if pa.types.is_int32(arrow) or pa.types.is_uint16(arrow):
        return "integer"
    if pa.types.is_integer(arrow):
        return "bigint"
    if pa.types.is_float16(arrow) or pa.types.is_float32(arrow):
        return "real"
    if pa.types.is_floating(arrow):
        return "double precision"
    if pa.types.is_decimal(arrow):
        return f"numeric({arrow.precision},{arrow.scale})"
    if pa.types.is_date(arrow):
        return "date"
    if pa.types.is_time(arrow):
        return "time"
    if pa.types.is_timestamp(arrow):
        return "timestamptz" if arrow.tz is not None else "timestamp"
    if (
        pa.types.is_string(arrow)
        or pa.types.is_large_string(arrow)
        or pa.types.is_string_view(arrow)
        or pa.types.is_dictionary(arrow)
    ):
        return "text"
    if (
        pa.types.is_binary(arrow)
        or pa.types.is_large_binary(arrow)
        or pa.types.is_binary_view(arrow)
        or pa.types.is_fixed_size_binary(arrow)
    ):
        return "bytea"
    raise ValueError(f"a column of {arrow} has no temporary-table type")


def temporary_columns(schema: pa.Schema) -> list[tuple[str, str]]:
    """The columns of a temporary table created by an ingest of ``schema``: each declared as its
    Arrow type's PostgreSQL type and landed as the temporary tables land it."""
    from provisa.pgwire.temp_exec import landed_type

    return [(f.name, landed_type(f.name, _temporary_type(f.type))) for f in schema]


#: The rows one INSERT of an ingest writes.
INGEST_BATCH_ROWS = 500


def _as_instants(rows: pa.Table) -> pa.Table:
    """``rows`` with each timestamp column as the instants PostgreSQL holds: a zoned value in UTC
    (the same instant; in its own zone a value near the range's ends falls outside the years a
    value can be read in), and a nanosecond one at the microsecond, truncated."""
    import pyarrow.compute as pc

    columns = []
    for field, column in zip(rows.schema, rows.columns, strict=True):
        kind = field.type
        if pa.types.is_timestamp(kind) and (kind.tz is not None or kind.unit == "ns"):
            unit = "us" if kind.unit == "ns" else kind.unit
            target = pa.timestamp(unit, tz="UTC" if kind.tz is not None else None)
            column = pc.cast(column, target, safe=False)
        columns.append(column)
    return pa.table(columns, names=rows.column_names)


def ingest_statements(table: str, schema: str | None, rows: pa.Table) -> list[str]:
    """``rows`` as INSERT ... VALUES statements into ``table`` (in ``schema`` when it is named),
    :data:`INGEST_BATCH_ROWS` rows each, every value its SQL literal."""
    from provisa.compiler.sql_literals import sql_literal

    def quoted(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    rows = _as_instants(rows)
    target = quoted(table) if schema is None else f"{quoted(schema)}.{quoted(table)}"
    columns = ", ".join(quoted(c) for c in rows.column_names)
    statements = []
    for start in range(0, rows.num_rows, INGEST_BATCH_ROWS):
        chunk = rows.slice(start, INGEST_BATCH_ROWS).to_pylist()
        values = ", ".join(
            "(" + ", ".join(sql_literal(row[c], "postgres") for c in rows.column_names) + ")"
            for row in chunk
        )
        statements.append(f"INSERT INTO {target} ({columns}) VALUES {values}")
    return statements


def update_result(count: int) -> bytes:
    """An update's answer: DoPut's metadata, its affected count."""
    return pb.DoPutUpdateResult(record_count=count).SerializeToString()


def described_schema(shape: list[tuple[str, str]]) -> pa.Schema | None:
    """The Arrow schema of a statement described as ``shape`` (its result columns and their
    types, as pgwire's Describe derives them), or None when a column's type has no Arrow type
    Provisa names. The protocol makes a prepared statement's dataset schema the server's best
    guess and lets it be absent, which None is."""
    from provisa.core.catalog_types import _physical_type_to_arrow

    fields = []
    for name, column_type in shape:
        try:
            fields.append(pa.field(name, _physical_type_to_arrow(column_type)))
        except KeyError:
            return None
    return pa.schema(fields)


def serialized_schema(schema: pa.Schema | None) -> bytes:
    """``schema`` as the IPC-encapsulated bytes a Flight SQL message carries; none for None."""
    return b"" if schema is None else schema.serialize().to_pybytes()


#: A placeholder's type oid (provisa.pgwire.parameters) as the Arrow type of its parameter.
_PARAMETER_ARROW = {
    25: pa.utf8(),
    23: pa.int32(),
    20: pa.int64(),
    16: pa.bool_(),
    701: pa.float64(),
}


def parameter_schema(query: str) -> pa.Schema:
    """``query``'s parameters: one per ``$N`` placeholder, named for it and typed as pgwire's
    ParameterDescription types it."""
    from provisa.pgwire.parameters import placeholder_oids

    return pa.schema(
        [
            pa.field(f"${i}", _PARAMETER_ARROW[oid])
            for i, oid in enumerate(placeholder_oids(query), 1)
        ]
    )


def prepared_result(handle: bytes, query: str, schema: pa.Schema | None) -> bytes:
    """CreatePreparedStatement's answer: the handle, the statement's schema when it is known, and
    its parameters' (:func:`parameter_schema`)."""
    return pack(
        pb.ActionCreatePreparedStatementResult(
            prepared_statement_handle=handle,
            dataset_schema=serialized_schema(schema),
            parameter_schema=parameter_schema(query).serialize().to_pybytes(),
        )
    )


def prepared_info(
    descriptor: flight.FlightDescriptor,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    handle: bytes,
) -> flight.FlightInfo:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A prepared statement's FlightInfo: one endpoint whose ticket runs it, with the rows
    carrying their schema (as :func:`statement_info`)."""
    ticket = pack(pb.CommandPreparedStatementQuery(prepared_statement_handle=handle))
    endpoint = flight.FlightEndpoint(ticket, [])  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    return flight.FlightInfo(None, descriptor, [endpoint], -1, -1)  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__


#: The session options Provisa has: the session's current catalog and schema, under the names
#: the ADBC Flight SQL driver reads and sets them by.
SESSION_OPTIONS = ("catalog", "schema")


def session_options(options: dict[str, str]) -> bytes:
    """GetSessionOptions' answer: the session's ``options``, each a string."""
    value = flight_pb.SessionOptionValue
    result = flight_pb.GetSessionOptionsResult(
        session_options={name: value(string_value=v) for name, v in options.items()}
    )
    return result.SerializeToString()


def session_options_set(refused: dict[str, flight_pb.SetSessionOptionsResult.ErrorValue]) -> bytes:
    """SetSessionOptions' answer: each option refused, by name, with why (an ErrorValue)."""
    error = flight_pb.SetSessionOptionsResult.Error
    result = flight_pb.SetSessionOptionsResult(
        errors={name: error(value=why) for name, why in refused.items()}
    )
    return result.SerializeToString()


def session_closed() -> bytes:
    """CloseSession's answer: the session is closed."""
    return flight_pb.CloseSessionResult(
        status=flight_pb.CloseSessionResult.CLOSED
    ).SerializeToString()


#: The one transaction id Provisa answers BeginTransaction with: a transaction holds nothing
#: (REQ-883), so every one is the same.
NO_OP_TRANSACTION = b"provisa-no-op"


def transaction_begun() -> bytes:
    """BeginTransaction's answer: the id of a transaction that holds nothing (REQ-883)."""
    return pack(pb.ActionBeginTransactionResult(transaction_id=NO_OP_TRANSACTION))
