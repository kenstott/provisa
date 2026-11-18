# Copyright (c) 2026 Kenneth Stott
# Canary: e7a23f68-1af9-47b4-a9ac-285ca7ad57f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of text search (REQ-2167, REQ-2171): ``ROWS``, which
provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms, pg_forms_textsearch
from provisa.transpiler.rows.base import _TRINO, Macro, _macro

_TSVECTOR: tuple[Macro, ...] = (
    _macro(
        "tsvector_strip",
        "value",
        pg_forms.TSVECTOR_STRIP,
        {_TRINO: pg_forms.TRINO_TSVECTOR_STRIP},
    ),
    _macro("strip", "value", pg_forms.TSVECTOR_STRIP, {_TRINO: pg_forms.TRINO_TSVECTOR_STRIP}),
    _macro(
        "tsvector_length",
        "value",
        pg_forms.TSVECTOR_LENGTH,
        {_TRINO: pg_forms.TRINO_TSVECTOR_LENGTH},
    ),
    _macro("tsvector_unnest", "value", pg_forms.TSVECTOR_UNNEST),
)

ROWS: tuple[Macro, ...] = (
    *_TSVECTOR,
    *(Macro(name, params, forms) for name, params, forms in pg_forms_textsearch.ROWS),
)
