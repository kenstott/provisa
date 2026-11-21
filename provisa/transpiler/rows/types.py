# Copyright (c) 2026 Kenneth Stott
# Canary: 8a0708af-9ff9-4d43-879e-11b16febf6c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of type operators and type input and output (REQ-2167, REQ-2171):
``ROWS``, which provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms_type_io, pg_forms_types
from provisa.transpiler.rows.base import Macro, _macro, _operator

ROWS: tuple[Macro, ...] = (
    _macro(
        "uuid_extract_version",
        "value",
        "CASE WHEN substr(CAST(value AS VARCHAR), 20, 1) IN ('8', '9', 'a', 'b') "
        "THEN uuid_extract_version(value) END",
    ),
    *(Macro(name, params, forms) for name, params, forms in pg_forms_type_io.ROWS),
    *map(_operator, pg_forms_types.OPERATORS),
)
