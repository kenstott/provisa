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

import json
from decimal import Decimal

from provisa.compiler.sql_gen import ColumnRef


class _Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            f = float(o)
            if f == int(f) and "." not in str(o):
                return int(f)
            return f
        return super().default(o)


def rows_to_ndjson(
    rows: list[tuple],
    columns: list[ColumnRef],
) -> str:
    """Serialize rows to NDJSON (one JSON object per line).

    Returns flat objects — no nesting. Each row is a dict of column_name → value.
    """
    lines: list[str] = []
    for row in rows:
        obj: dict = {}
        for i, col in enumerate(columns):
            key = f"{col.nested_in}.{col.field_name}" if col.nested_in else col.field_name
            obj[key] = row[i]
        lines.append(json.dumps(obj, cls=_Encoder))
    return "\n".join(lines) + ("\n" if lines else "")
