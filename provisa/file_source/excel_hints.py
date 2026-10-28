# Copyright (c) 2026 Kenneth Stott
# Canary: 19b92abf-8feb-4fd8-b609-2a79deac15a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A files source's hints for how the file adapter's Excel reader finds the tables of its
workbooks: ``mapping.excel_hints``, given as the adapter's ``excelHints`` operand.

Each hint overrides one rule the reader otherwise applies; a hint left unset leaves its rule as
it is (the form shows it as Auto-detect). They are given for every workbook of the source, for
one workbook by its file name (``workbooks``), and for one table by the name the reader gives it,
``<file>__<sheet>`` (``tables``); the narrowest that sets a hint wins::

    excel_hints:
      max_header_rows: 1
      workbooks:
        watch_list.xlsx: {column_gap: 1}
      tables:
        report__summary: {header_rows: 2}
        report__totals: {range: "summary!H3:K20"}

The gaps between tables are found before the tables are, so ``table_gap_rows`` and
``column_gap`` are given for every workbook or for one; ``range`` names the cells of one table.
A hint the reader does not take is refused by name, as the adapter would refuse it.
"""

from __future__ import annotations

import re
from typing import Any

#: Each count hint, by its name in the mapping and in the adapter's operand.
COUNTS = {
    "header_rows": "headerRows",
    "max_header_rows": "maxHeaderRows",
    "table_gap_rows": "tableGapRows",
    "column_gap": "columnGap",
    "min_columns": "minColumns",
    "min_data_rows": "minDataRows",
}
#: The hints a workbook (or every workbook) takes.
WORKBOOK_HINTS = tuple(COUNTS)
#: The hints a table takes.
TABLE_HINTS = ("header_rows", "max_header_rows", "min_columns", "min_data_rows", "range")

# A block of one sheet: Sheet!A3:F200, or 'Sheet name'!A3:F200.
_RANGE = re.compile(
    r"^(?:'(?:[^']|'')+'|[^'!:\s][^!:]*)!\$?[A-Za-z]{1,3}\$?\d+:\$?[A-Za-z]{1,3}\$?\d+$"
)


class InvalidExcelHints(ValueError):
    """A files source's Excel hints the reader cannot take; the message names the hint."""


def _level(source_id: str, where: str, hints: Any, allowed: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(hints, dict):
        raise InvalidExcelHints(f"files source {source_id!r}: {where} must be an object")
    unknown = sorted(set(hints) - set(allowed))
    if unknown:
        raise InvalidExcelHints(
            f"files source {source_id!r}: {where}: unknown hint(s) {unknown}; "
            f"known: {sorted(allowed)}"
        )
    operand: dict[str, Any] = {}
    for name, value in hints.items():
        if name == "range":
            if not isinstance(value, str) or not _RANGE.match(value.strip()):
                raise InvalidExcelHints(
                    f"files source {source_id!r}: {where}.range {value!r} is not a range of one "
                    "sheet, such as Sheet1!A3:F200"
                )
            operand["range"] = value.strip()
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise InvalidExcelHints(
                f"files source {source_id!r}: {where}.{name} must be a whole number of 1 or "
                f"more, not {value!r}"
            )
        operand[COUNTS[name]] = value
    return operand


def excel_hints_operand(source_id: str, mapping: dict | None) -> dict | None:
    """The file adapter's ``excelHints`` operand for a source's mapping, or None when the source
    gives no hint."""
    hints = (mapping or {}).get("excel_hints")
    if hints is None:
        return None
    if not isinstance(hints, dict):
        raise InvalidExcelHints(
            f"files source {source_id!r}: mapping.excel_hints must be an object"
        )
    own = {k: v for k, v in hints.items() if k not in ("workbooks", "tables")}
    operand = _level(source_id, "excel_hints", own, WORKBOOK_HINTS)
    for group, allowed in (("workbooks", WORKBOOK_HINTS), ("tables", TABLE_HINTS)):
        entries = hints.get(group)
        if entries is None:
            continue
        if not isinstance(entries, dict):
            raise InvalidExcelHints(
                f"files source {source_id!r}: excel_hints.{group} must be an object"
            )
        levels = {
            name: _level(source_id, f"excel_hints.{group}.{name}", entry, allowed)
            for name, entry in entries.items()
        }
        levels = {name: level for name, level in levels.items() if level}
        if levels:
            operand[group] = levels
    return operand or None
