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

import os
from dataclasses import replace

from provisa.compiler.sql_gen import CompiledQuery
from provisa.compiler.stage2 import _apply_limit_ceiling, resolve_row_cap

DEFAULT_SAMPLE_SIZE = 100


def get_sample_size() -> int:  # REQ-005
    """Legacy admin-settings knob (deprecated). Distinct from the governance row cap
    (``resolve_row_cap`` → ``default_row_limit``) and from the large-result redirect
    threshold. Retained only for the admin `default_sample_size` surface."""
    return int(os.environ.get("PROVISA_SAMPLE_SIZE", str(DEFAULT_SAMPLE_SIZE)))


def apply_sampling(compiled: CompiledQuery, sample_size: int) -> CompiledQuery:  # REQ-263, REQ-478
    """Return a copy with the query's LIMIT injected/capped to ``sample_size``."""
    return replace(compiled, sql=_apply_limit_ceiling(compiled.sql, sample_size))


def apply_sampling_if_needed(compiled: CompiledQuery, role) -> CompiledQuery:  # REQ-005, REQ-263
    """Return a copy with the role's row cap applied (no cap for FULL_RESULTS roles)."""
    cap = resolve_row_cap(role)
    return (
        compiled if cap is None else replace(compiled, sql=_apply_limit_ceiling(compiled.sql, cap))
    )
