# Copyright (c) 2026 Kenneth Stott
# Canary: 91a60fd8-a005-4502-b0ae-45d029d640ea
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of JSON (REQ-2167, REQ-2171): ``ROWS``, which provisa.transpiler.rows
collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms_json
from provisa.transpiler.rows.base import Macro, _macro, _udf

ROWS: tuple[Macro, ...] = (
    _udf("pg_json_object", "value", "CAST(provisa_json_object(value, NULL) AS JSON)"),
    _macro(
        "pg_json_object",
        "ks, vs",
        "CAST(CASE WHEN ks IS NOT NULL AND vs IS NOT NULL THEN "
        "provisa_json_object(ks, vs) END AS JSON)",
    ),
    *(Macro(name, params, forms) for name, params, forms in pg_forms_json.ROWS),
)
