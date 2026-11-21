# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8a51c7-0e6f-4b29-a7d4-c25e9f1b8063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""money's text form (REQ-2236; replayed against postgres:17.11 by the function support measure,
REQ-2171).

money is held as its numeric base (provisa.transpiler.pg_money), which writes cash_out and a cast
of money to text as ``money_text`` of that numeric: this row is that call's form. cash_out is
strict, so a NULL money is NULL: Trino's format writes a NULL as Java does, null, cut to the two
digits asked for ('$nu'), so Trino's form answers a NULL before it formats. A dialect with no form
of it refuses the call by name, as pg_money refused it before the row was in the grid."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal

FAMILY = "money"

#: The dialects whose forms are Trino's SQL.
_TRINO = ("trino", "presto", "athena")

_CENTS = "CAST(round(($v) * 100, 0) AS BIGINT)"
_WIDE_CENTS = f"CAST({_CENTS} AS HUGEINT)"

_DUCKDB = (
    f"CASE WHEN {_CENTS} < 0 THEN '-' ELSE '' END || '$' || "
    f"format('{{:,}}', abs({_WIDE_CENTS}) // 100) || '.' || "
    f"printf('%02d', CAST(abs({_WIDE_CENTS}) % 100 AS INTEGER))"
)
_TRINO_FORM = (
    "CASE WHEN $v IS NULL THEN CAST(NULL AS VARCHAR) "
    "ELSE CASE WHEN $v < 0 THEN '-' ELSE '' END || "
    "format('$%,.2f', abs(CAST($v AS DECIMAL(19,2)))) END"
)


def _refused(dialect: str) -> Refusal:
    """The refusal of money's text on a dialect with no form of it. provisa.transpiler.pg_money
    raises it as the subtype's refusal, which carries no SQLSTATE of its own: the client receives
    XX000."""
    return Refusal(
        "XX000",
        f"cast to text on a money column needs money's own semantics, which {dialect} neither "
        "has nor emulates; it is not answered over the column's text form",
    )


MONEY_TEXT = Feature(
    "money_text",
    FAMILY,
    "money's text, as cash_out writes it: $1,234.50, NULL for a NULL.",
    read=Side(
        rules=(
            Rule(
                "money_text($v)",
                {
                    **{d: _refused(d) for d in DIALECTS if d not in NATIVE},
                    "duckdb": _DUCKDB,
                    **dict.fromkeys(_TRINO, _TRINO_FORM),
                },
            ),
        )
    ),
)

FEATURES: tuple[Feature, ...] = (MONEY_TEXT,)
