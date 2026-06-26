# Copyright (c) 2026 Kenneth Stott
# Canary: c7d8b553-cb60-4fa8-ab67-acf149d4b964
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Row-cap adapters over the single Stage 2 cap implementation.

The row cap is governance, not statistical sampling. The one implementation lives in
``provisa.compiler.stage2`` (``resolve_row_cap`` / ``apply_row_cap``); the helpers here
adapt it to ``CompiledQuery`` for callers that have not been migrated to Stage 2, and
expose the configured default for the admin settings API. Real statistical sampling is a
user query feature (GraphQL ``sample`` arg → ``TABLESAMPLE``), handled in the compiler.
"""

# Requirements: REQ-005, REQ-263, REQ-478

from __future__ import annotations

from dataclasses import replace

import os

from provisa.compiler.sql_gen import CompiledQuery
from provisa.compiler.stage2 import _apply_limit_ceiling, resolve_row_cap

DEFAULT_SAMPLE_SIZE: int = 10000


def get_sample_size() -> int:
    """Return the configured sample size from PROVISA_SAMPLE_SIZE env var."""
    raw = os.environ.get("PROVISA_SAMPLE_SIZE")
    if raw is not None:
        return int(raw)
    return DEFAULT_SAMPLE_SIZE


def apply_sampling(compiled: CompiledQuery, sample_size: int) -> CompiledQuery:  # REQ-263, REQ-478
    """Return a copy with the query's LIMIT injected/capped to ``sample_size``."""
    return replace(compiled, sql=_apply_limit_ceiling(compiled.sql, sample_size))


def apply_sampling_if_needed(compiled: CompiledQuery, role) -> CompiledQuery:  # REQ-005, REQ-263
    """Return a copy with the role's row cap applied (no cap for FULL_RESULTS roles)."""
    cap = resolve_row_cap(role)
    return (
        compiled if cap is None else replace(compiled, sql=_apply_limit_ceiling(compiled.sql, cap))
    )
