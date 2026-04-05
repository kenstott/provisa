# Copyright (c) 2026 Kenneth Stott
# Canary: 13fbb4d0-69b5-46e2-af68-f3bbda539ef7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow IPC buffer serialization (REQ-051)."""

from __future__ import annotations

import io
from decimal import Decimal

import pyarrow as pa

from provisa.compiler.sql_gen import ColumnRef


def _column_names(columns: list[ColumnRef]) -> list[str]:
    return [
        f"{col.nested_in}.{col.field_name}" if col.nested_in else col.field_name
        for col in columns
    ]


def _convert(val):
    if isinstance(val, Decimal):
        return float(val)
    return val


def rows_to_arrow_ipc(
    rows: list[tuple],
    columns: list[ColumnRef],
) -> bytes:
    """Serialize rows to Arrow IPC format (stream)."""
    names = _column_names(columns)
    col_data: dict[str, list] = {name: [] for name in names}
    for row in rows:
        for i, name in enumerate(names):
            col_data[name].append(_convert(row[i]))

    table = pa.table(col_data)
    buf = io.BytesIO()
    writer = pa.ipc.new_stream(buf, table.schema)
    writer.write_table(table)
    writer.close()
    return buf.getvalue()


def rows_to_arrow_table(
    rows: list[tuple],
    columns: list[ColumnRef],
) -> pa.Table:
    """Convert rows to a PyArrow Table (for Arrow Flight)."""
    names = _column_names(columns)
    col_data: dict[str, list] = {name: [] for name in names}
    for row in rows:
        for i, name in enumerate(names):
            col_data[name].append(_convert(row[i]))
    return pa.table(col_data)
