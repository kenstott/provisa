# Copyright (c) 2026 Kenneth Stott
# Canary: 6ed5b35b-df0a-4ec9-bb78-5e09ef3864b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code source's rows on the wire, as Arrow (REQ-2098).

Anything that returns rows -- a @table, a @stream, a @command that returns a collection -- sends
them as Arrow IPC record batches, built by the host program against the schema Provisa gives the
call: the function's row type, as discovery read it (:func:`row_schema`). A row type with fields
is a struct of them, a list a list, an enum its text. Two values Arrow has no exact form for are
text with the IR type in the field's metadata: a ``numeric`` (its digits, exact) and a ``json``
value (the code-source wire's JSON of it). A row type that is loose -- the columns found by a
preview -- is one ``json`` field holding each row whole (:data:`WHOLE_ROW`).

:func:`rows_of` reads a batch back into rows as the rest of Provisa takes them.
"""

# Requirements: REQ-2098
from __future__ import annotations

import base64
import json
from decimal import Decimal
from typing import Any

import pyarrow as pa

from provisa.codesource.manifest import (
    Anonymous,
    EnumOf,
    Export,
    ListOf,
    Manifest,
    Named,
    Scalar,
    Shape,
)

#: The field metadata key naming the IR type a text field carries.
IR_KEY = b"provisa.ir"
#: The one field of a loose row type: each row whole, as JSON.
WHOLE_ROW = "$row"

_SCALARS: dict[str, pa.DataType] = {
    "text": pa.utf8(),
    "double": pa.float64(),
    "bigint": pa.int64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}
#: IR types sent as text, named in the field's metadata.
_AS_TEXT = frozenset({"numeric", "json"})


def _field(name: str, shape: Shape, manifest: Manifest) -> pa.Field:
    if isinstance(shape, Scalar):
        if shape.ir in _AS_TEXT:
            return pa.field(name, pa.utf8(), metadata={IR_KEY: shape.ir.encode()})
        if shape.ir not in _SCALARS:
            raise TypeError(f"field {name!r}: the IR type {shape.ir!r} has no Arrow form")
        return pa.field(name, _SCALARS[shape.ir])
    if isinstance(shape, EnumOf):
        return pa.field(name, pa.utf8())
    if isinstance(shape, ListOf):
        return pa.field(name, pa.list_(_field("item", shape.element, manifest)))
    if isinstance(shape, Named):
        fields = manifest.types[shape.name].fields
        return pa.field(name, pa.struct([_field(f.name, f.shape, manifest) for f in fields]))
    if isinstance(shape, Anonymous):
        return pa.field(name, pa.struct([_field(f.name, f.shape, manifest) for f in shape.fields]))
    raise TypeError(f"field {name!r}: no Arrow form for {shape!r}")


def row_schema(manifest: Manifest, export: Export) -> pa.Schema:
    """The schema of ``export``'s rows: its row type's fields, or one whole-row JSON field when
    the row type is loose (found by a preview)."""
    shape = export.returns
    if isinstance(shape, Named):
        fields = manifest.types[shape.name].fields
    elif isinstance(shape, Anonymous):
        fields = shape.fields
    else:
        return pa.schema([pa.field(WHOLE_ROW, pa.utf8(), metadata={IR_KEY: b"json"})])
    return pa.schema([_field(f.name, f.shape, manifest) for f in fields])


def schema_text(schema: pa.Schema) -> str:
    """``schema`` as a call message carries it: an Arrow IPC stream of it with no batches,
    base64-encoded -- the form every language's Arrow library reads a schema from."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass  # the schema alone
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()


def _back(value: Any, field: pa.Field) -> Any:
    from provisa.codesource.worker import decode

    if value is None:
        return None
    ir = (field.metadata or {}).get(IR_KEY)
    if ir == b"numeric":
        return Decimal(value)
    if ir == b"json":
        return decode(json.loads(value))
    kind = field.type
    if pa.types.is_struct(kind):
        return {
            kind.field(i).name: _back(value[kind.field(i).name], kind.field(i))
            for i in range(kind.num_fields)
        }
    if pa.types.is_list(kind):
        return [_back(v, kind.value_field) for v in value]
    return value


def rows_of(ipc: bytes) -> list[dict]:
    """The rows of one Arrow IPC stream a host sent."""
    reader = pa.ipc.open_stream(pa.py_buffer(ipc))
    table = reader.read_all()
    schema = table.schema
    if schema.names == [WHOLE_ROW]:
        field = schema.field(WHOLE_ROW)
        return [_back(row[WHOLE_ROW], field) for row in table.to_pylist()]
    fields = [schema.field(i) for i in range(len(schema))]
    return [{f.name: _back(row[f.name], f) for f in fields} for row in table.to_pylist()]
