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
catalog DB (REQ-2167, PGWIRE'S CATALOG FUNCTIONS): ``provisa_size_pretty``, pg_size_pretty's text
(DuckDB's of that name writes KiB), and the UDFs of provisa.federation.duckdb_pg_scalars, where a
value needs PostgreSQL's own algorithm. A function a DuckDB expression carries is no function here:
it is a rewrite in provisa.transpiler.function_macros, as money's, the shapes', tsvector's and
abbrev are.

sha224, sha384 and sha512 are the crypto extension's ``crypto_hash`` (REQ-2171, EXTENSIONS AND TOTAL
LANDING). :func:`register` only loads it, from the extension directory: staging it is an install's
step (provisa-duckdb-ext, or ``INSTALL crypto FROM community``), so a connection never reaches the
network. Where it is not staged the call is refused by name."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from typing import Any

from provisa.transpiler.function_macros import FunctionRefused


def _refuse_digest(algorithm: str, value: bytes) -> bytes:
    """crypto_hash where the extension is not staged: the function the statement called, refused."""
    raise FunctionRefused(algorithm.replace("sha2-", "sha"), "duckdb (no crypto extension)")


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
    from provisa.federation.duckdb_extensions import load_staged
    from provisa.federation.duckdb_pg_scalars import register as register_scalars
    from provisa.federation.duckdb_pg_xml import register as register_xml

    register_scalars(con)
    register_xml(con)
    con.create_function(
        "provisa_size_pretty", size_pretty, ["HUGEINT"], "VARCHAR", side_effects=False
    )
    if not load_staged(con, "crypto"):  # refused by name at the call (REQ-2171)
        con.create_function(
            "crypto_hash", _refuse_digest, ["VARCHAR", "BLOB"], "BLOB", side_effects=False
        )
