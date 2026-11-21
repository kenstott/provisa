# Copyright (c) 2026 Kenneth Stott
# Canary: 06fe516a-69c2-4da5-9f63-f938dd6bc97c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of money (REQ-2167, REQ-2171): ``ROWS``, which provisa.transpiler.rows
collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms
from provisa.transpiler.rows.base import _TRINO, Macro, _macro

#: Money over its numeric base, for provisa.transpiler.pg_money: no name of PostgreSQL's, so these
#: never read a statement's own call. money_text is the polyfill grid's (polyfill.money).
_MONEY: tuple[Macro, ...] = (
    _macro("money_words", "value", pg_forms.MONEY_WORDS),
    _macro("money_bytes", "value", pg_forms.MONEY_BYTES, own={_TRINO: pg_forms.TRINO_MONEY_BYTES}),
    _macro("money_input", "value", pg_forms.MONEY_INPUT),
)

ROWS: tuple[Macro, ...] = (*_MONEY,)
