# Copyright (c) 2026 Kenneth Stott
# Canary: a4d71c06-93be-4e25-8f17-5b2c60e8d9f1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each engine does with the statements of pg17_forms_answers: replayed, never derived. A
statement not listed answers as PostgreSQL 17 did. REFUSED is by name; DIFFERS is an answer
PostgreSQL did not give; ERROR is the engine's own refusal of what PostgreSQL answered."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import datetime
import decimal
import math
import struct
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts" / "conformance"))

from function_support import difference  # noqa: E402

REFUSED, DIFFERS, ERROR = "refused", "differs", "error"
_REAL = 700


def compared(oids: list[int], want: list[tuple], rows: list[tuple], values: bool, sql: str) -> bool:
    """Whether ``rows`` are PostgreSQL's answer, as function_support compares one."""
    if any(len(row) != len(oids) for row in rows):
        return False
    left, right = {"fields": oids, "rows": want}, {"fields": oids, "rows": rows}
    return difference(left, right, values=values, sql=sql) is None


def _real_text(value: float) -> str:
    """A real as float4out writes it: the fewest digits that read back as the same value."""
    for digits in range(1, 10):
        short = f"{value:.{digits}g}"
        if struct.pack("f", float(short)) == struct.pack("f", value):
            return repr(float(short)).removesuffix(".0")
    return repr(value)


def text_of(value: Any, oid: int) -> str | None:
    """A cell of an engine's driver as PostgreSQL's text."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, float) and not math.isfinite(value):
        return {True: "NaN"}.get(math.isnan(value), "Infinity" if value > 0 else "-Infinity")
    if isinstance(value, float):
        return _real_text(value) if oid == _REAL else repr(value).removesuffix(".0")
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    if isinstance(value, datetime.datetime):
        moment = value.astimezone(datetime.UTC) if value.tzinfo else value
        fraction = f".{moment.microsecond:06d}".rstrip("0") if moment.microsecond else ""
        return moment.strftime("%Y-%m-%d %H:%M:%S") + fraction + ("+00" if value.tzinfo else "")
    if isinstance(value, datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray):
        return "\\x" + bytes(value).hex()
    if isinstance(value, list | tuple):
        return "{" + ",".join("NULL" if v is None else str(text_of(v, 0)) for v in value) + "}"
    return str(value)


# fmt: off
MATRIX: dict[str, dict[str, str]] = {"clickhouse": {}, "trino": {}}
