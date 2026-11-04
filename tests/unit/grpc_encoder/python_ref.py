# Copyright (c) 2026 Kenneth Stott
# Canary: 5d99c36e-8f1f-47c1-a182-9d398c6271dc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The two sides of the gRPC differential (REQ-2194), and the service they are driven over.

The wired path (``rust_encode``): Arrow batches -> ``provisa.grpc.server._row_encoder`` (the
encoder every query RPC builds, provisa._encoders.grpc) -> the serialized messages the handler
yields. This is what gRPC sends.

The reference (``python_encode``): the same batches read into Python rows as the product read them
before REQ-2194 (``column_values`` per column), each row's cells through the frozen Python path in
``oracle.py`` (``_kwargs_for`` restated, a read mask's selection by ``restrict_json``), then
``msg_cls(**kwargs)``, ``{Type}Batch(rows=...)`` and ``SerializeToString``. Where the encoder is
meant to answer otherwise the reference is corrected to the answer it should give (``expected``
below, each an intentional difference):

- a timestamp in a named zone is read in that zone as zoneinfo reads it (J498), not by pytz,
  which pyarrow's ``to_pylist`` takes and which has no daylight time after 2037;
- a nanosecond timestamp in a Timestamp field keeps its nanoseconds (pandas' Timestamp, which
  ``to_pylist`` makes of one, gave FromDatetime only its microseconds);
- a finite value past a float or double field's range (a numeric's text such as ``1E+400``, a
  decimal) is refused by name (J502b), where float() made it an infinity and protobuf sent that.

Service: the product's ``generate_proto`` over a two-table SchemaInput covering every proto type
the type map produces, a many-to-one and a one-to-many relation, once for the wire (every role's
union, ``authoritative``) and once for a role that is not granted ``orders.secret``, through one
FieldNumberAllocator, as a schema build does.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import timezone
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
from google.protobuf.descriptor import FieldDescriptor

from provisa.core.ir_arrow import column_values
from provisa.core.ir_types import infinity_of
from provisa.grpc.descriptor import OrgDescriptor
from provisa.grpc.query_ir import restrict_json
from provisa.grpc.server import _row_encoder
from tests.unit.grpc_encoder import oracle

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
            "enable_group_by": True,  # its {Type}GroupByRow holds a group's nodes
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
        self.wire = OrgDescriptor(generate_proto(_schema_input(WIRE), numbers, authoritative=True))
        self.role = OrgDescriptor(generate_proto(_schema_input(ROLE), numbers))


def out_cols(msg_cls: Any, column_names: list[str]) -> list[str]:
    """server._handle_query_bound: result columns to proto field names, by the same key."""

    def _norm(s: str) -> str:
        return s.replace("_", "").lower()

    proto_by_norm = {_norm(f.name): f.name for f in msg_cls.DESCRIPTOR.fields}
    return [proto_by_norm.get(_norm(c), c) for c in column_names]


# ---- the reference --------------------------------------------------------------------------


def _named_zone(arrow_type: pa.DataType) -> str | None:
    if not pa.types.is_timestamp(arrow_type) or arrow_type.tz is None:
        return None
    tz = arrow_type.tz
    if tz in ("UTC", "Z") or (len(tz) == 6 and tz[0] in "+-" and tz[3] == ":"):
        return None
    return tz


def _values(column: pa.Array) -> list[Any]:
    """column_values, a named zone read by zoneinfo (J498: expected)."""
    zone = _named_zone(column.type)
    if zone is None or column.type.unit == "ns":
        return column_values(column)
    utc = column_values(column.cast(pa.timestamp(column.type.unit, "UTC")))
    named = ZoneInfo(zone)
    return [
        v if v is None or infinity_of(v) else v.astimezone(timezone.utc).astimezone(named)
        for v in utc
    ]


_FLOATS = (FieldDescriptor.TYPE_FLOAT, FieldDescriptor.TYPE_DOUBLE)


def _expected_value(field: Any, value: Any) -> Any:
    """oracle._proto_value, corrected where the encoder answers otherwise (see the module)."""
    if field.type == FieldDescriptor.TYPE_MESSAGE and hasattr(value, "nanosecond"):
        # a nanosecond timestamp (pandas' Timestamp) keeps its nanoseconds
        from google.protobuf.message_factory import GetMessageClass

        ts: Any = GetMessageClass(field.message_type)()
        ts.FromNanoseconds(value.value)
        return ts
    proto: Any = oracle._proto_value(field, value)
    if field.type in _FLOATS and math.isinf(proto) and not _named_infinity(value):
        raise ValueError(f'column "{field.name}": {value!r} is out of range (J502b)')
    return proto


def _named_infinity(value: Any) -> bool:
    if isinstance(value, float):
        return True
    if isinstance(value, (str, bytes)):
        text = value.decode() if isinstance(value, bytes) else value
        return text.strip().lstrip("+-").lower().startswith("inf")
    return False


def _kwargs_for(col_fields: list[tuple[str, Any, Any]], row: tuple) -> dict:
    """server._handle_query_bound._kwargs_for, as it stood (a read mask's selection with it)."""
    kwargs = {}
    for i, (col, field, selection) in enumerate(col_fields):
        if i < len(row) and row[i] is not None:
            value = row[i] if selection is None else restrict_json(row[i], selection)
            kwargs[col] = _expected_value(field, value) if field is not None else value
    return kwargs


def python_encode(
    service: OrgDescriptor,
    type_name: str,
    batches: Iterable[pa.RecordBatch],
    *,
    batch_rows: int = 0,
    masks: list | None = None,
) -> list[bytes]:
    """The serialized response messages: one per row (Query{Type}), or ``{Type}Batch`` messages
    of ``batch_rows`` rows (Query{Type}Batch)."""
    batches = list(batches)
    msg_cls = getattr(service, type_name)
    cols = out_cols(msg_cls, list(batches[0].schema.names))
    descriptor = msg_cls.DESCRIPTOR
    selections = masks or [None] * len(cols)
    col_fields = [
        (col, descriptor.fields_by_name.get(col), sel)
        for col, sel in zip(cols, selections, strict=True)
    ]
    rows: list[Any] = []
    for b in batches:
        if not b.num_rows:
            continue
        # every column is read before any message of the batch is built, as before
        columns = [_values(c) for c in b.columns]
        rows.extend(msg_cls(**_kwargs_for(col_fields, row)) for row in zip(*columns))
    if not batch_rows:
        return [m.SerializeToString() for m in rows]
    batch_cls = getattr(service, f"{type_name}Batch")
    return [
        batch_cls(rows=rows[i : i + batch_rows]).SerializeToString()
        for i in range(0, len(rows), batch_rows)
    ]


# ---- the wired encoder ----------------------------------------------------------------------


def rust_encode(
    service: OrgDescriptor,
    type_name: str,
    batches: Iterable[pa.RecordBatch],
    *,
    batch_rows: int = 0,
    masks: list | None = None,
) -> list[bytes]:
    batches = list(batches)
    msg_cls = getattr(service, type_name)
    encoder = _row_encoder(
        msg_cls,
        out_cols(msg_cls, list(batches[0].schema.names)),
        masks,
        getattr(service, f"{type_name}Batch") if batch_rows else None,
        batch_rows,
    )
    out: list[bytes] = []
    for b in batches:
        out.extend(encoder.encode(b))
    out.extend(encoder.finish())
    return out


def rows_of(batch: pa.RecordBatch) -> list[tuple]:
    """A batch as the row result it would be (REQ-2195): each cell the value column_values gives."""
    return list(zip(*(column_values(c) for c in batch.columns), strict=True))


def has_map(batch: pa.RecordBatch) -> bool:
    """Whether a column holds an Arrow map at any depth. column_values gives a map as a list of
    (key, value) tuples, which no row source holds (its map is a dict): such a batch has no row
    form to compare (test_rows.py covers a row source's dicts)."""
    return any("map<" in str(f.type) for f in batch.schema)


def rows_encode(
    service: OrgDescriptor,
    type_name: str,
    rows: Iterable[list[Any]],
    names: list[str],
    *,
    batch_rows: int = 0,
    masks: list | None = None,
    types: list[str | None] | None = None,
) -> list[bytes]:
    """The wired encoder fed a row result's batches (``encode_rows``), as grpc/server does."""
    msg_cls = getattr(service, type_name)
    encoder = _row_encoder(
        msg_cls,
        out_cols(msg_cls, names),
        masks,
        getattr(service, f"{type_name}Batch") if batch_rows else None,
        batch_rows,
    )
    out: list[bytes] = []
    for batch in rows:
        out.extend(encoder.encode_rows(batch, types))
    out.extend(encoder.finish())
    return out


def node_reference(service: OrgDescriptor, type_name: str, nodes: list[dict]) -> list[bytes]:
    """A group's nodes as the frozen _dict_row_to_message built them."""
    msg_cls = getattr(service, type_name)
    return [
        oracle._dict_row_to_message(service, msg_cls, node).SerializeToString() for node in nodes
    ]
