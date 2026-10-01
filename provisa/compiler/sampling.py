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
from typing import Any


from provisa.compiler.sql_gen import CompiledQuery
from provisa.compiler.stage2 import _apply_limit_ceiling, resolve_row_cap
from provisa.core import settings_registry


def __getattr__(name: str) -> Any:
    # REQ-1913: DEFAULT_SAMPLE_SIZE is declared once, in the settings catalog — looked up when
    # asked for, so importing this module does not load the settings declarations.
    if name == "DEFAULT_SAMPLE_SIZE":
        return settings_registry.setting("sampling.default_sample_size").default
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_sample_size() -> int:
    """The sample size: the value set through the admin API when there is one (a control-plane
    row every worker reads, REQ-1900), else ``PROVISA_SAMPLE_SIZE``, else the default."""
    return settings_registry.value("sampling.default_sample_size")


def apply_sampling(compiled: CompiledQuery, sample_size: int) -> CompiledQuery:  # REQ-263, REQ-478
    """Return a copy with the query's LIMIT injected/capped to ``sample_size``."""
    return replace(compiled, sql=_apply_limit_ceiling(compiled.sql, sample_size))


def apply_sampling_if_needed(compiled: CompiledQuery, role) -> CompiledQuery:  # REQ-005, REQ-263
    """Return a copy with the role's row cap applied (no cap for FULL_RESULTS roles)."""
    cap = resolve_row_cap(role)
    return (
        compiled if cap is None else replace(compiled, sql=_apply_limit_ceiling(compiled.sql, cap))
    )
