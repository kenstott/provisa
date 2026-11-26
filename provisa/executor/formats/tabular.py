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
from provisa.core.numeric_type import (
    decimal_text,
    numeric_arrow_type,
    numeric_size,
    numeric_type,
)

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


def _decimal_digits(val: Decimal, name: str) -> tuple[int, int]:
    """The (integer digits, scale) a finite Decimal carries (REQ-1961)."""
    sign_digits_exp = val.as_tuple()
    exponent = sign_digits_exp.exponent
    if not isinstance(exponent, int):
        raise ValueError(f"column {name!r}: {val} is not a finite decimal and has no Parquet form")
    scale = max(0, -exponent)
    return max(0, len(sign_digits_exp.digits) + exponent), scale


def _values_type(values: list, name: str) -> pa.DataType | None:
    """REQ-1961: the exact decimal type of a column with no declared type whose values are
    Decimals: the widest precision and scale its values carry. None when it holds no Decimal."""
    sizes = [_decimal_digits(v, name) for v in values if isinstance(v, Decimal)]
    if not sizes:
        return None
    scale = max(s for _, s in sizes)
    precision = max(1, max(i for i, _ in sizes) + scale)
    return numeric_arrow_type(numeric_type(precision, scale))


def rows_to_parquet(  # REQ-049, REQ-050, REQ-1961
    rows: list[tuple],
    columns: list[ColumnRef],
) -> bytes:
    """Serialize rows to Parquet (denormalized/flat).

    REQ-1961: a column whose ColumnRef carries a declared numeric type (e.g. ``numeric(18,2)``)
    is written as that exact decimal. A column with no declared size whose values are Decimals
    is written as the exact decimal its values need: the widest precision and scale among them.
    A Decimal is never written as a float.
    """
    names, flat_rows = _resolve_columns(rows, columns)

    # _resolve_columns may expand dict-cells into new dot-notation names with no ColumnRef: such
    # a name declares no type, as a ColumnRef with data_type None does.
    declared = {
        name: col.data_type for col, name in zip(columns, _column_names(columns), strict=True)
    }

    arrays: list[pa.Array] = []
    for i, name in enumerate(names):
        values = [row[i] for row in flat_rows]
        data_type = declared.get(name)
        arrow_type = (
            numeric_arrow_type(data_type)
            if numeric_size(data_type) is not None
            else _values_type(values, name)
        )
        arrays.append(pa.array(values, type=arrow_type))

    table = pa.table(dict(zip(names, arrays, strict=True)))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()
