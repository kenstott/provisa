# Copyright (c) 2026 Kenneth Stott
# Canary: 66103854-72fc-4d40-b583-5f1ff03a6dbc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of aggregates (REQ-2167, REQ-2171): ``ROWS``, which
provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler.rows.base import Macro, _pg

ROWS: tuple[Macro, ...] = (_pg("every", "x", "BOOL_AND(x)"),)
