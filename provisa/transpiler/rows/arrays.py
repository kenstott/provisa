# Copyright (c) 2026 Kenneth Stott
# Canary: 5b103bfa-7919-45e5-a1b3-9c8ac31969d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of arrays (REQ-2167, REQ-2171): ``ROWS``, which provisa.transpiler.rows
collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler import pg_forms_types
from provisa.transpiler.rows.base import _DUCKDB, _TRINO, Forms, Macro, Pg, _macro

#: The array operators' forms on Trino, which has none of PostgreSQL's: arraycontains (``@>``) is
#: NULL of a NULL array, false where the second holds a NULL (a NULL element is equal to nothing),
#: and otherwise whether the first holds each of the second's elements (a NULL of the first is
#: equal to none of them, so CONTAINS' NULL there is false).
_ARRAY_OPERATORS_ON_TRINO: dict[str, Forms] = {
    "arraycontains": {
        _TRINO: "CASE WHEN a IS NULL OR b IS NULL THEN NULL "
        "ELSE all_match(b, _x -> _x IS NOT NULL AND coalesce(contains(a, _x), FALSE)) END"
    },
}
#: An array operator the semantic statement writes as PostgreSQL's operator, read on Trino as the
#: row of the table it is: a range's, a jsonb's and a subtyped column's were written before the
#: functions (transpile), so what is left is an array's.
_ARRAY_OPERATOR_ROWS: dict[type[exp.Expr], str] = {exp.ArrayContainsAll: "arraycontains"}

ROWS: tuple[Macro, ...] = (
    _macro("array_remove", "a, v", "list_filter(a, _x -> _x IS DISTINCT FROM v)"),
    *(
        Macro(
            form.name,
            tuple(form.params.split(", ")),
            operators={_DUCKDB: Pg(form.sql), **_ARRAY_OPERATORS_ON_TRINO.get(form.name, {})},
        )
        for form in pg_forms_types.ARRAY_OPERATORS
    ),
)
