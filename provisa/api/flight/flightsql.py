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


def _server_info() -> dict[int, str | bool]:
    """What Provisa answers of SqlInfo: the server's name and versions, and that it takes SQL."""
    return {
        pb.FLIGHT_SQL_SERVER_NAME: "Provisa",
        pb.FLIGHT_SQL_SERVER_VERSION: importlib.metadata.version("provisa"),
        pb.FLIGHT_SQL_SERVER_ARROW_VERSION: pa.__version__,
        pb.FLIGHT_SQL_SERVER_SQL: True,
    }


def sql_info_table(command: pb.CommandGetSqlInfo) -> pa.Table:
    """CommandGetSqlInfo's answer: each info Provisa has, of those asked (all when none is)."""
    known = _server_info()
    asked = list(command.info) or sorted(known)
    names: list[int] = []
    strings: list[str] = []
    bools: list[bool] = []
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
        else:
            type_ids.append(0)
            offsets.append(len(strings))
            strings.append(value)
    union_type = SQL_INFO_SCHEMA.field("value").type
    children = [
        pa.array(strings, pa.utf8()),
        pa.array(bools, pa.bool_()),
        pa.array([], pa.int64()),
        pa.array([], pa.int32()),
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


def prepared_result(handle: bytes, schema: pa.Schema | None) -> bytes:
    """CreatePreparedStatement's answer: the handle, and the statement's schema when it is
    known."""
    return pack(
        pb.ActionCreatePreparedStatementResult(
            prepared_statement_handle=handle, dataset_schema=serialized_schema(schema)
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


def session_options(catalog: str, schema: str) -> bytes:
    """GetSessionOptions' answer: the session's current ``catalog`` and ``schema``, under the
    option names the ADBC Flight SQL driver reads them by."""
    value = flight_pb.SessionOptionValue
    result = flight_pb.GetSessionOptionsResult(
        session_options={
            "catalog": value(string_value=catalog),
            "schema": value(string_value=schema),
        }
    )
    return result.SerializeToString()
