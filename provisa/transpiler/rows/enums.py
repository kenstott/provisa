# Copyright (c) 2026 Kenneth Stott
# Canary: 4219758d-efc9-4dac-94c8-274963caf029
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of enums (REQ-2167, REQ-2171): ``ROWS``, which provisa.transpiler.rows
collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.core.pg_error import raising
from provisa.transpiler import pg_forms_enum, pg_forms_types
from provisa.transpiler.rows.base import _ORDER, Macro, _macro


def _enum_ordered(answer: str) -> str:
    """``answer`` over two values of an enum, in its labels' order. A value the engine holds as
    text has lost that order: compared, it would answer the alphabet's (REQ-2171, AN ANSWER THAT
    DIFFERS IS REFUSED)."""
    refused = raising("0A000", f"'enum order {pg_forms_types.REFUSED}'")
    return f"CASE WHEN typeof(a) = 'VARCHAR' THEN {refused} ELSE {answer} END"


_ENUMS: tuple[Macro, ...] = (
    *(
        _macro(f"enum_{suffix}", "a, b", _enum_ordered(f"a {operator} b"))
        for suffix, operator in _ORDER.items()
    ),
    _macro(
        "enum_cmp",
        "a, b",
        _enum_ordered("CASE WHEN a < b THEN -1 WHEN a > b THEN 1 WHEN a = b THEN 0 END"),
    ),
    _macro("enum_larger", "a, b", _enum_ordered("CASE WHEN a > b THEN a WHEN a <= b THEN b END")),
    _macro("enum_smaller", "a, b", _enum_ordered("CASE WHEN a < b THEN a WHEN a >= b THEN b END")),
    # DuckDB's own, under PostgreSQL's names: only an engine with enum types has a form.
    _macro("enum_first", "a", "enum_first(a)"),
    _macro("enum_last", "a", "enum_last(a)"),
    _macro("enum_range", "a", "enum_range(a)"),
    _macro("enum_range", "a, b", "enum_range_boundary(a, b)"),
)

ROWS: tuple[Macro, ...] = (
    *_ENUMS,
    *(Macro(name, params, forms) for name, params, forms in pg_forms_enum.ROWS),
)
