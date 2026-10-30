# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1e4b70-3a96-4c27-b5f1-72c0e96d1a48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a money value needs on DuckDB beyond its numeric base (REQ-2167, REQ-2171): money's text
forms. provisa.transpiler.pg_money writes every other money operation over the base."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import struct
from decimal import Decimal
from typing import Any

from provisa.core.pg_error import END, MARK, PostgresError
from provisa.core.pg_money import cash_in, cash_out, cash_words


def _input(text: str) -> Decimal:
    """money's input, as the base's value."""
    try:
        return Decimal(cash_in(text)).scaleb(-2)
    except PostgresError as refused:
        raise ValueError(f"{MARK} {refused.sqlstate}: {refused}{END}") from refused


def _bytes(cents: int) -> bytes:
    """cash_send: the int8, big-endian."""
    return struct.pack("!q", cents)


def register(con: Any) -> None:
    """Create the UDFs on DuckDB connection ``con``."""
    con.create_function("provisa_cash_in", _input, ["VARCHAR"], "DECIMAL(21,4)", side_effects=False)
    con.create_function("provisa_cash_out", cash_out, ["BIGINT"], "VARCHAR", side_effects=False)
    con.create_function("provisa_cash_words", cash_words, ["BIGINT"], "VARCHAR", side_effects=False)
    con.create_function("provisa_cash_send", _bytes, ["BIGINT"], "BLOB", side_effects=False)
