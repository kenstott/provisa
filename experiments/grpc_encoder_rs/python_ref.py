# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f4c1e-9d3a-4e6b-8f05-6a1c3e9d7b42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's own Python gRPC encode path, driven in isolation, and the service it is driven
over.

Encode path (provisa/grpc/server.py, ``_handle_query_bound`` and ``_handle_query_batch``): Arrow
batches -> ``stream_rows_from_arrow`` (``column_values`` per column, ``zip`` to row tuples) -> per
row ``_kwargs_for`` (``_proto_value`` per non-null cell) -> ``msg_cls(**kwargs)`` ->
``SerializeToString`` (what ``_streaming_rpc`` hands gRPC). Query{Type}Batch wraps those rows in
``{Type}Batch(rows=...)`` messages of ``batch_rows`` each. A relationship-shaped row (a struct is a
many-to-one relation's dict, a list of structs a one-to-many's) goes through
``ProvisaServicer._dict_row_to_message``, the product's nested-relationship encoder.

``_kwargs_for`` and the column-to-field mapping (``_norm`` / ``_proto_by_norm``) are closures inside
``_handle_query_bound``; they are restated here line for line (no read mask). ``_proto_value``,
``_dict_row_to_message``, the message classes and the descriptor are the product's own.

Service: the product's ``generate_proto`` over a two-table SchemaInput (every proto type the type
map produces, a many-to-one and a one-to-many relationship), once for the wire (every role's
union, ``authoritative``) and once for a role that is not granted ``orders.secret``, through one
FieldNumberAllocator, as a schema build does.

Run with the worktree root and build/ on PYTHONPATH.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from functools import cached_property
from typing import Any

import pyarrow as pa

from provisa.federation.runtime_support import stream_rows_from_arrow
from provisa.grpc.descriptor import OrgDescriptor
from provisa.grpc.schema_gen import compile_descriptor_set
from provisa.grpc.server import ProvisaServicer, _proto_value

PACKAGE = "provisa.v1"
WIRE, ROLE = "wire", "seller"

# (column, registered type); the comment is the proto type the product maps it to
ORDERS_COLUMNS: list[tuple[str, str]] = [
    ("id", "bigint"),  # int64
    ("qty", "integer"),  # int32
    ("small", "smallint"),  # int32
    ("price", "double"),  # double
    ("ratio", "float"),  # float
    ("amount", "decimal(18,2)"),  # double
    ("name", "varchar"),  # string
    ("created", "timestamp"),  # google.protobuf.Timestamp
    ("created_tz", "timestamp with time zone"),  # google.protobuf.Timestamp
    ("day", "date"),  # string
    ("at", "time"),  # string
    ("span", "interval"),  # string
    ("payload", "json"),  # string
    ("active", "boolean"),  # bool
    ("blob", "varbinary"),  # bytes
    ("tag", "uuid"),  # string
    ("secret", "varchar"),  # string, the wire's only: the role is not granted it
    ("customer_id", "bigint"),  # int64
]
CUSTOMERS_COLUMNS: list[tuple[str, str]] = [
    ("id", "bigint"),
    ("name", "varchar"),
    ("joined", "timestamp"),
]


def _schema_input(role_id: str) -> Any:
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_types import SchemaInput

    def column(name: str) -> dict:
        granted = [WIRE] if name == "secret" else [WIRE, ROLE]
        return {
            "column_name": name,
            "visible_to": granted,
            "scope": "domain",
            "native_filter_type": None,
        }

    def table(table_id: int, name: str, columns: list[tuple[str, str]]) -> dict:
        return {
            "id": table_id,
            "domain_id": "sales",
            "source_id": "pg",
            "schema_name": "public",
            "table_name": name,
            "columns": [column(c) for c, _ in columns],
            "write_ops": [],
            "write_returns_rows": True,
            "write_refused_forms": [],
        }

    def metadata(columns: list[tuple[str, str]]) -> list:
        return [ColumnMetadata(column_name=c, data_type=t, is_nullable=True) for c, t in columns]

    return SchemaInput(
        tables=[table(1, "orders", ORDERS_COLUMNS), table(2, "customers", CUSTOMERS_COLUMNS)],
        relationships=[
            {
                "id": "orders_customer",
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            },
            {
                "id": "customer_orders",
                "source_table_id": 2,
                "target_table_id": 1,
                "source_column": "id",
                "target_column": "customer_id",
                "cardinality": "one-to-many",
            },
        ],
        column_types={1: metadata(ORDERS_COLUMNS), 2: metadata(CUSTOMERS_COLUMNS)},
        naming_rules=[],
        role={"id": role_id, "description": "x", "domain_access": ["sales"], "capabilities": []},
        domains=[{"id": "sales"}],
    )


class Service:
    """The wire service and the role's, compiled as the product compiles them."""

    def __init__(self) -> None:
        from provisa.grpc.field_numbering import FieldNumberAllocator
        from provisa.grpc.proto_gen import generate_proto

        numbers = FieldNumberAllocator([])
        self.wire_proto = generate_proto(_schema_input(WIRE), numbers, authoritative=True)
        self.role_proto = generate_proto(_schema_input(ROLE), numbers)
        self.wire = OrgDescriptor(self.wire_proto)
        self.role = OrgDescriptor(self.role_proto)

    @cached_property
    def descriptor_set(self) -> bytes:
        """The wire service's serialized FileDescriptorSet: what the Rust encoder is given."""
        return compile_descriptor_set(self.wire_proto)


def out_cols(msg_cls: Any, column_names: list[str]) -> list[str]:
    """server._handle_query_bound: result columns to proto field names, by the same key."""

    def _norm(s: str) -> str:
        return s.replace("_", "").lower()

    _proto_by_norm = {_norm(f.name): f.name for f in msg_cls.DESCRIPTOR.fields}
    return [_proto_by_norm.get(_norm(c), c) for c in column_names]


def _kwargs_for(col_fields: list[tuple[str, Any]], row: tuple) -> dict:
    """server._handle_query_bound._kwargs_for, without a read mask."""
    kwargs = {}
    for i, (col, field) in enumerate(col_fields):
        if i < len(row) and row[i] is not None:
            value = row[i]
            kwargs[col] = _proto_value(field, value) if field is not None else value
    return kwargs


class _Servicer:
    """What ProvisaServicer._dict_row_to_message reads of its servicer: the bound service, and
    itself (it recurses through ``self``)."""

    _dict_row_to_message = ProvisaServicer._dict_row_to_message

    def __init__(self, service: OrgDescriptor) -> None:
        self._pb2 = service


def row_messages(
    service: OrgDescriptor,
    type_name: str,
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    mode: str = "row",
) -> Iterator[Any]:
    """The {Type} messages the product builds for these batches, one per row."""
    msg_cls = getattr(service, type_name)
    stream = stream_rows_from_arrow(schema, iter(batches))
    cols = out_cols(msg_cls, list(stream.column_names))
    if mode == "node":
        servicer = _Servicer(service)
        for batch in stream.batches():
            for row in batch:
                yield servicer._dict_row_to_message(msg_cls, dict(zip(cols, row, strict=True)))
        return
    descriptor = msg_cls.DESCRIPTOR
    col_fields = [(col, descriptor.fields_by_name.get(col)) for col in cols]
    for batch in stream.batches():
        for row in batch:
            yield msg_cls(**_kwargs_for(col_fields, row))


def python_encode(
    service: OrgDescriptor,
    type_name: str,
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    *,
    mode: str = "row",
    batch_rows: int = 0,
) -> list[bytes]:
    """The serialized response messages: one per row (Query{Type}), or ``{Type}Batch`` messages
    of ``batch_rows`` rows (Query{Type}Batch)."""
    rows = row_messages(service, type_name, schema, batches, mode)
    if not batch_rows:
        return [m.SerializeToString() for m in rows]
    batch_cls = getattr(service, f"{type_name}Batch")
    out: list[bytes] = []
    rows_buf: list = []
    for m in rows:
        rows_buf.append(m)
        if len(rows_buf) >= batch_rows:
            out.append(batch_cls(rows=rows_buf).SerializeToString())
            rows_buf = []
    if rows_buf:
        out.append(batch_cls(rows=rows_buf).SerializeToString())
    return out


def rust_encoder(
    svc: Service,
    type_name: str,
    schema: pa.Schema,
    *,
    mode: str = "row",
    batch_rows: int = 0,
) -> Any:
    import grpc_encoder_rs

    msg_cls = getattr(svc.wire, type_name)
    return grpc_encoder_rs.Encoder(
        svc.descriptor_set,
        f"{PACKAGE}.{type_name}",
        out_cols(msg_cls, list(schema.names)),
        mode=mode,
        batch_message=f"{PACKAGE}.{type_name}Batch" if batch_rows else None,
        batch_rows=batch_rows,
    )


def rust_encode(
    svc: Service,
    type_name: str,
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    *,
    mode: str = "row",
    batch_rows: int = 0,
) -> list[bytes]:
    enc = rust_encoder(svc, type_name, schema, mode=mode, batch_rows=batch_rows)
    out: list[bytes] = []
    for b in batches:
        if b.num_rows:  # stream_rows_from_arrow skips an empty batch
            out.extend(enc.encode(b))
    out.extend(enc.finish())
    return out
