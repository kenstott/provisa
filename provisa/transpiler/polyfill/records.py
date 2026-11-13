# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3f6e21-4c8a-4d07-b5e2-71a0c8d4f95e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The equality of two records (REQ-2236; replayed against postgres:17.11 by the function support
measure, REQ-2171).

record_eq and record_ne are a record's btree comparison: a NULL field equals a NULL field, so
``record_eq(ROW(NULL, 'c'), ROW(NULL, 'c'))`` is true, where SQL's ``=`` of two rows is NULL. A
record's image comparison takes a NULL field so too. Each is strict: a NULL record answers NULL.
DuckDB's ``=`` of two structs already takes a NULL field as equal; Trino's ``IS NOT DISTINCT FROM``
of two rows does. Every other dialect is sent the form it had (``=``).

A record's order (record_lt and its kin, and their image forms) is refused by name on Trino: its
comparison of two rows refuses a field that is NULL (run 43307392116), where PostgreSQL orders a
NULL field after any value, and a text field orders by the database's collation there."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side

FAMILY = "records"

#: The dialects whose forms are Trino's SQL.
_TRINO = ("trino", "presto", "athena")


def _trino(test: str) -> str:
    """Trino's form: NULL for a NULL record, else ``test`` of the two rows."""
    return f"CASE WHEN $a IS NULL OR $b IS NULL THEN CAST(NULL AS BOOLEAN) ELSE $a {test} $b END"


def _compared(name: str, summary: str, operator: str, trino_test: str) -> Feature:
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(
                Rule(
                    f"{name}($a, $b)",
                    {
                        **{d: f"($a) {operator} ($b)" for d in DIALECTS if d not in NATIVE},
                        **dict.fromkeys(_TRINO, _trino(trino_test)),
                    },
                ),
            )
        ),
    )


RECORD_EQ = _compared(
    "record_eq",
    "Two records equal, a NULL field equal to a NULL field.",
    "=",
    "IS NOT DISTINCT FROM",
)
RECORD_NE = _compared(
    "record_ne",
    "Two records not equal, a NULL field equal to a NULL field.",
    "<>",
    "IS DISTINCT FROM",
)
RECORD_IMAGE_EQ = _compared(
    "record_image_eq",
    "Two records of equal images, a NULL field equal to a NULL field.",
    "=",
    "IS NOT DISTINCT FROM",
)
RECORD_IMAGE_NE = _compared(
    "record_image_ne",
    "Two records of images not equal, a NULL field equal to a NULL field.",
    "<>",
    "IS DISTINCT FROM",
)

_ORDER = {"lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_ORDER_WORDS = {
    "lt": "before",
    "le": "before or equal to",
    "gt": "after",
    "ge": "after or equal to",
}
_ORDER_LIMITATION = Limitation(
    "Refused by name: Trino's comparison of two rows refuses a NULL field, which PostgreSQL orders "
    "after any value, and orders a text field by its code points."
)


def _ordered(name: str, summary: str, operator: str) -> Feature:
    """An order of two records: Trino refuses it by name, every other dialect the form it had."""
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(
                Rule(
                    f"{name}($a, $b)",
                    {
                        **{d: f"($a) {operator} ($b)" for d in DIALECTS if d not in NATIVE},
                        **trino.refused_on_trino(name),
                    },
                ),
            ),
            limitations=dict.fromkeys(_TRINO, (_ORDER_LIMITATION,)),
        ),
    )


ORDERS: tuple[Feature, ...] = tuple(
    _ordered(
        f"{stem}{order}",
        f"One record{' image' if stem == 'record_image_' else ''} {_ORDER_WORDS[order]} another.",
        operator,
    )
    for stem in ("record_", "record_image_")
    for order, operator in _ORDER.items()
)

FEATURES: tuple[Feature, ...] = (RECORD_EQ, RECORD_NE, RECORD_IMAGE_EQ, RECORD_IMAGE_NE, *ORDERS)
