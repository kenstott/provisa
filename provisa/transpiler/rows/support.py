# Copyright (c) 2026 Kenneth Stott
# Canary: bcd4ddf4-0755-4186-aeab-c03bcee143bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of aggregate state and index support functions (REQ-2167, REQ-2171):
``ROWS``, which provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms_aggregate, pg_forms_index
from provisa.transpiler.rows.base import Macro, _operator

ROWS: tuple[Macro, ...] = (
    *(_operator(form) for form in pg_forms_aggregate.STATE_FUNCTIONS),
    *(_operator(form) for form in pg_forms_aggregate.FINAL_FUNCTIONS),
    *(_operator(form) for form in pg_forms_index.SUPPORT_FUNCTIONS),
    *(
        Macro(name, tuple(params.split(", ")), {frozenset({d}): sql for d, sql in forms.items()})
        for (name, params), forms in pg_forms_index.per_engine().items()
    ),
)
