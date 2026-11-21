# Copyright (c) 2026 Kenneth Stott
# Canary: 06f76e54-d2ca-4f39-904f-3b6f7871c03e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""NDJSON streaming format — one JSON object per line (REQ-048)."""

from __future__ import annotations

from provisa.compiler.sql_gen import ColumnRef

# Requirements: REQ-048, REQ-1867


def rows_to_ndjson(  # REQ-048
    rows: list[tuple],
    columns: list[ColumnRef],
) -> str:
    """Serialize rows to NDJSON (one JSON object per line).

    Returns flat objects — no nesting. Each row is a dict of column_name → value, each value as
    the JSON answer writes it (serialize.column_value, REQ-1867).
    """
    from provisa.api.json_response import dumps
    from provisa.executor.serialize import column_value

    lines: list[str] = []
    for row in rows:
        obj: dict = {}
        for i, col in enumerate(columns):
            key = f"{col.nested_in}.{col.field_name}" if col.nested_in else col.field_name
            obj[key] = column_value(row[i], col)
        # The JSON answer's writer: a number inside JSON text keeps its digits.
        lines.append(dumps(obj).decode())
    return "\n".join(lines) + ("\n" if lines else "")
