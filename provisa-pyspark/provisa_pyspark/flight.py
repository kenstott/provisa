# Copyright (c) 2026 Kenneth Stott
# Canary: 460f9a1d-a5ab-470d-a15f-d1833840d341
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Provisa read over Arrow Flight SQL (REQ-2270), without Spark.

The table's declared schema is GetSchema of the statement (Provisa types it from the registered
metadata, as the principal would be answered it); its endpoints are GetFlightInfo of the statement;
each endpoint's rows are DoGet of its ticket. Every call carries the caller's headers, so every
one is governed and audited as that principal.

The one Flight SQL message sent, CommandStatementQuery, is encoded here (FlightSql.proto field 1,
``query``, wrapped in a ``google.protobuf.Any``) rather than taking a protobuf dependency for it.
"""

# Requirements: REQ-2270

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.flight as fl

from provisa_pyspark.options import ReadOptions
from provisa_pyspark.spark_types import spark_type

_STATEMENT_QUERY = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
#: A location that says "the server this info came from" (Flight's reuse-connection URI).
_REUSE = "arrow-flight-reuse-connection://"


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _length_delimited(field: int, payload: bytes) -> bytes:
    return _varint(field << 3 | 2) + _varint(len(payload)) + payload


def statement_command(query: str) -> bytes:
    """The FlightDescriptor command of a Flight SQL statement query."""
    message = _length_delimited(1, query.encode("utf-8"))
    return _length_delimited(1, _STATEMENT_QUERY) + _length_delimited(2, message)


@dataclass(frozen=True)
class Endpoint:
    """Where one part of the rows is fetched: its ticket and the server holding it."""

    ticket: bytes
    uri: str


def _call(options: ReadOptions) -> fl.FlightCallOptions:  # pyright: ignore[reportPrivateImportUsage]
    return fl.FlightCallOptions(headers=options.headers())  # pyright: ignore[reportPrivateImportUsage]


def _descriptor(options: ReadOptions) -> fl.FlightDescriptor:  # pyright: ignore[reportPrivateImportUsage]
    return fl.FlightDescriptor.for_command(statement_command(options.statement()))  # pyright: ignore[reportPrivateImportUsage]


def declared_schema(options: ReadOptions) -> pa.Schema:
    """The table's columns as the principal is served them, typed as declared."""
    client = fl.connect(options.uri)  # pyright: ignore[reportPrivateImportUsage]
    try:
        return client.get_schema(_descriptor(options), _call(options)).schema
    finally:
        client.close()


def endpoints(options: ReadOptions) -> list[Endpoint]:
    """The endpoints the table's rows are fetched from, each one a part of them."""
    client = fl.connect(options.uri)  # pyright: ignore[reportPrivateImportUsage]
    try:
        info = client.get_flight_info(_descriptor(options), _call(options))
    finally:
        client.close()
    return [Endpoint(e.ticket.ticket, _endpoint_uri(options, e)) for e in info.endpoints]


def _endpoint_uri(options: ReadOptions, endpoint: fl.FlightEndpoint) -> str:  # pyright: ignore[reportPrivateImportUsage]
    """The server an endpoint's ticket is fetched from: the one the info came from when the
    endpoint names none (or names that one), else the first location it names (Flight: any of
    them serves the ticket)."""
    if not endpoint.locations:
        return options.uri
    uri = endpoint.locations[0].uri.decode("utf-8")
    return options.uri if uri.startswith(_REUSE) else uri


def batches(options: ReadOptions, endpoint: Endpoint) -> Iterator[pa.RecordBatch]:
    """The record batches of one endpoint, as the server sends them."""
    client = fl.connect(endpoint.uri)  # pyright: ignore[reportPrivateImportUsage]
    try:
        reader = client.do_get(fl.Ticket(endpoint.ticket), _call(options)).to_reader()  # pyright: ignore[reportPrivateImportUsage]
        yield from reader
    finally:
        client.close()


class SchemaDrift(ValueError):
    """Rows arrived in a shape other than the table's declared one."""


def conformed(batch: pa.RecordBatch, declared: pa.Schema, target: pa.Schema) -> pa.RecordBatch:
    """``batch`` in ``target``'s Arrow layout (what Spark reads the declared types as). Its columns
    must be the declared ones, in order, each of a type whose Spark type is the declared column's;
    the cast between them then changes only the layout (a string's offsets, a list's field name, a
    timestamp's zone label), and a cast that would lose a value raises rather than truncate."""
    if batch.schema.names != declared.names:
        raise SchemaDrift(
            f"rows arrived with columns {batch.schema.names}, the table declares {declared.names}"
        )
    for field, want in zip(batch.schema, declared):
        got = spark_type(field.type, field.name)
        if got != spark_type(want.type, want.name):
            raise SchemaDrift(
                f"column {field.name!r} arrived as {field.type}, declared as {want.type}"
            )
    if batch.schema.equals(target):
        return batch
    return batch.cast(target, safe=True)
