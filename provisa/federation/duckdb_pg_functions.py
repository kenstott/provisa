# Copyright (c) 2026 Kenneth Stott
# Canary: 328bd3f6-efa4-4648-babc-d0f1c5aca13c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's functions DuckDB has no equivalent of, on the embedded DuckDB engine and the pgwire
catalog DB (REQ-2167, PGWIRE'S CATALOG FUNCTIONS): ``provisa_sha224``, ``provisa_sha384`` and
``provisa_sha512``, each a bytea's digest as a bytea (sha256 is DuckDB's own,
provisa.transpiler.pg_functions), ``provisa_size_pretty``, pg_size_pretty's text (DuckDB's of that
name writes KiB), and the functions of provisa.federation.duckdb_pg_scalars: UDFs where a value
needs PostgreSQL's own algorithm, and macros where a DuckDB expression carries the function."""

# Requirements: REQ-2167

from __future__ import annotations

import hashlib
from typing import Any


def _digest(algorithm: str):
    def digest(value: bytes) -> bytes:
        return hashlib.new(algorithm, value).digest()

    return digest


#: pg_size_pretty's units, as src/backend/utils/adt/dbsize.c lists them: (name, the size below
#: which it is used, whether the value is half-rounded, its power of two).
_SIZE_UNITS = (
    ("bytes", 10 * 1024, False, 0),
    ("kB", 20 * 1024 - 1, True, 10),
    ("MB", 20 * 1024 - 1, True, 20),
    ("GB", 20 * 1024 - 1, True, 30),
    ("TB", 20 * 1024 - 1, True, 40),
    ("PB", 20 * 1024 - 1, True, 50),
)


def _truncated_shift(value: int, bits: int) -> int:
    """value / 2**bits, truncated toward zero as C's integer division is."""
    quotient = abs(value) >> bits
    return quotient if value >= 0 else -quotient


def size_pretty(size: int) -> str:
    """pg_size_pretty(size), by dbsize.c's walk of its units."""
    for index, (name, limit, rounded, bits) in enumerate(_SIZE_UNITS):
        following = _SIZE_UNITS[index + 1] if index + 1 < len(_SIZE_UNITS) else None
        if following is None or abs(size) < limit:
            if rounded:
                size = _truncated_shift(size + (1 if size >= 0 else -1), 1)
            return f"{size} {name}"
        size = _truncated_shift(size, following[3] - bits - following[2] + rounded)
    raise AssertionError("the last unit answers")


def register(con: Any) -> None:
    """Create the functions on DuckDB connection ``con``."""
    from provisa.federation.duckdb_pg_scalars import register as register_scalars

    from provisa.federation.duckdb_pg_ranges import register as register_ranges

    register_scalars(con)
    register_ranges(con)
    con.create_function(
        "provisa_size_pretty", size_pretty, ["HUGEINT"], "VARCHAR", side_effects=False
    )
    for algorithm in ("sha224", "sha384", "sha512"):
        con.create_function(
            f"provisa_{algorithm}", _digest(algorithm), ["BLOB"], "BLOB", side_effects=False
        )
