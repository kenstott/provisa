# Copyright (c) 2026 Kenneth Stott
# Canary: 38d1bfd8-197a-46ab-bfbe-a693136aae76
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of bytea and bit strings (REQ-2167, REQ-2171): ``ROWS``, which
provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.core.pg_error import raising
from provisa.transpiler.rows.base import Macro, _macro, _pg, _udf


def _in_bytea(index: str, size: str) -> str:
    """get_byte's and set_byte's refusal of an index outside the value."""
    return (
        f"WHEN {index} < 0 OR {index} >= {size} THEN "
        + raising(
            "2202E",
            f"'index ' || {index} || ' out of valid range, 0..' || ({size} - 1)",
        )
        + " "
    )


ROWS: tuple[Macro, ...] = (
    _pg("byteacat", "a, b", "CAST(a || b AS BYTEA)"),
    _udf("bytealike", "a, b", "provisa_bytea_like(a, b)"),
    _udf("byteanlike", "a, b", "NOT provisa_bytea_like(a, b)"),
    _pg("bitcat", "a, b", "CAST(CAST(a AS TEXT) || CAST(b AS TEXT) AS BIT)"),
    _macro(
        "bitshiftleft",
        "value, n",
        "CAST(substr(CAST(value AS VARCHAR), n + 1) || "
        "repeat('0', least(n, length(CAST(value AS VARCHAR)))) AS BIT)",
    ),
    _macro(
        "bitshiftright",
        "value, n",
        "CAST(repeat('0', least(n, length(CAST(value AS VARCHAR)))) || "
        "left(CAST(value AS VARCHAR), greatest(length(CAST(value AS VARCHAR)) - n, 0)) AS BIT)",
    ),
    _macro(
        "get_byte",
        "value, n",
        f"CASE {_in_bytea('n', 'octet_length(value)')}"
        "ELSE CAST('0x' || substr(hex(value), n * 2 + 1, 2) AS INTEGER) END",
    ),
    _macro(
        "set_byte",
        "value, n, new_byte",
        f"CASE {_in_bytea('n', 'octet_length(value)')}"
        "ELSE unhex(left(hex(value), n * 2) || printf('%02X', new_byte & 255) || "
        "substr(hex(value), n * 2 + 3)) END",
    ),
    _macro(
        "varbit",
        "value, n, explicit",
        "CASE WHEN value IS NOT NULL AND n IS NOT NULL THEN "
        "CAST(CASE WHEN n < 0 THEN CAST(value AS VARCHAR) ELSE left(CAST(value AS VARCHAR), n) END "
        "AS BIT) END",
    ),
)
