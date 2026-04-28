# Copyright (c) 2026 Kenneth Stott
# Canary: 363d8cdd-b270-4f8c-abfc-425d67b2c8ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tabular formats: CSV and Parquet — normalized and denormalized (REQ-049, REQ-050)."""

from __future__ import annotations

import csv
import io
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from provisa.compiler.sql_gen import ColumnRef


def _column_names(columns: list[ColumnRef]) -> list[str]:
    """Build flat column names, qualifying nested ones."""
    return [
        f"{col.nested_in}.{col.field_name}" if col.nested_in else col.field_name
        for col in columns
    ]


def _convert(val):
    if isinstance(val, Decimal):
        return float(val)
    return val


def rows_to_csv(
    rows: list[tuple],
    columns: list[ColumnRef],
) -> str:
    """Serialize rows to CSV (denormalized/flat)."""
    names = _column_names(columns)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(names)
    for row in rows:
        writer.writerow([_convert(v) for v in row])
    return buf.getvalue()


def rows_to_parquet(
    rows: list[tuple],
    columns: list[ColumnRef],
) -> bytes:
    """Serialize rows to Parquet (denormalized/flat)."""
    names = _column_names(columns)
    # Build columnar data
    col_data: dict[str, list] = {name: [] for name in names}
    for row in rows:
        for i, name in enumerate(names):
            col_data[name].append(_convert(row[i]))

    table = pa.table(col_data)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()
