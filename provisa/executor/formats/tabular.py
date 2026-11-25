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
from provisa.core.numeric_type import decimal_text, numeric_size

# Requirements: REQ-049, REQ-050, REQ-1961


def _column_names(columns: list[ColumnRef]) -> list[str]:
    """Build flat column names, qualifying nested ones."""
    return [
        f"{col.nested_in}.{col.field_name}" if col.nested_in else col.field_name for col in columns
    ]


def _convert_csv(val):
    # REQ-1961: a Decimal is written as its exact digits, never as float.
    if isinstance(val, Decimal):
        return decimal_text(val)
    return val


def _expand_cell(prefix: str, val, out: dict) -> None:
    """Recursively expand a dict or JSON-string cell into dot-notation keys."""
    if isinstance(val, dict):
        for k, v in val.items():
            _expand_cell(f"{prefix}.{k}", v, out)
    elif isinstance(val, str) and val.startswith("{") and val.endswith("}"):
        try:
            import json as _json

            parsed = _json.loads(val)
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    _expand_cell(f"{prefix}.{k}", v, out)
                return
        except (ValueError, TypeError):
            pass
        out[prefix] = val
    else:
        out[prefix] = val


def _flatten_dict_rows(  # REQ-049, REQ-050
    rows: list[tuple],
    col_names: list[str],
) -> tuple[list[str], list[tuple]]:
    """Re-expand rows where cells contain dicts into dot-notation columns."""
    all_keys: list[str] = []
    seen_keys: set[str] = set()
    flat_rows: list[dict] = []

    for row in rows:
        flat: dict = {}
        for name, val in zip(col_names, row):
            _expand_cell(name, val, flat)
        flat_rows.append(flat)
        for k in flat:
            if k not in seen_keys:
                seen_keys.add(k)
                all_keys.append(k)

    return all_keys, [tuple(fr.get(k) for k in all_keys) for fr in flat_rows]


def _resolve_columns(
    rows: list[tuple],
    columns: list[ColumnRef],
) -> tuple[list[str], list[tuple]]:
    """Return flat column names and rows, expanding dict/JSON-string cells when present."""
    names = _column_names(columns)
    if rows and any(
        isinstance(v, dict) or (isinstance(v, str) and v.startswith("{") and v.endswith("}"))
        for v in rows[0]
    ):
        return _flatten_dict_rows(rows, names)
    return names, rows


def rows_to_csv(  # REQ-049, REQ-050, REQ-1961
    rows: list[tuple],
    columns: list[ColumnRef],
) -> str:
    """Serialize rows to CSV (denormalized/flat)."""
    names, flat_rows = _resolve_columns(rows, columns)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(names)
    for row in flat_rows:
        writer.writerow([_convert_csv(v) for v in row])
    return buf.getvalue()


def rows_to_parquet(  # REQ-049, REQ-050, REQ-1961
    rows: list[tuple],
    columns: list[ColumnRef],
) -> bytes:
    """Serialize rows to Parquet (denormalized/flat).

    REQ-1961: a column whose ColumnRef carries a declared numeric type (e.g. ``numeric(18,2)``)
    is written as a ``decimal128(p,s)`` Parquet column.  A column with no declared type whose
    values happen to be Decimal is converted to float64 — the same rule as every other read path.
    """
    names, flat_rows = _resolve_columns(rows, columns)

    # Build a name→declared_size map for columns that carry a numeric type (REQ-1961).
    # _resolve_columns may expand dict-cells into new dot-notation names with no ColumnRef, so
    # any name not in the original set gets no size (None → float64 fallback).
    _orig_names = _column_names(columns)
    _decimal_sizes: dict[str, tuple[int, int] | None] = {}
    for col, name in zip(columns, _orig_names):
        _decimal_sizes[name] = numeric_size(col.data_type) if col.data_type else None

    col_data: dict[str, list] = {name: [] for name in names}
    for row in flat_rows:
        for i, name in enumerate(names):
            val = row[i]
            size = _decimal_sizes.get(name)
            if size is not None and isinstance(val, Decimal):
                col_data[name].append(val)  # kept as Decimal; typed below
            else:
                col_data[name].append(val if not isinstance(val, Decimal) else float(val))

    # Build the Arrow table, overriding the type for declared numeric columns.
    arrays: list[pa.Array] = []
    for name in names:
        size = _decimal_sizes.get(name)
        if size is not None:
            p, s = size
            arrays.append(pa.array(col_data[name], type=pa.decimal128(p, s)))
        else:
            arrays.append(pa.array(col_data[name]))

    table = pa.table(dict(zip(names, arrays)))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()
