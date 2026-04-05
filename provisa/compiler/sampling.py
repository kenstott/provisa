# Copyright (c) 2026 Kenneth Stott
# Canary: c7d8b553-cb60-4fa8-ab67-acf149d4b964
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sampling mode — cap result rows for roles without full_results capability.

Default behavior: all queries are sampled unless the role has full_results.
Sampling injects or caps LIMIT on compiled SQL.
"""

from __future__ import annotations

import os
import re

from provisa.compiler.sql_gen import CompiledQuery

DEFAULT_SAMPLE_SIZE = 100

_LIMIT_RE = re.compile(r'\bLIMIT\s+(\d+)', re.IGNORECASE)


def get_sample_size() -> int:
    """Get configured sample size from environment, or default."""
    return int(os.environ.get("PROVISA_SAMPLE_SIZE", str(DEFAULT_SAMPLE_SIZE)))


def apply_sampling(compiled: CompiledQuery, sample_size: int) -> CompiledQuery:
    """Apply sampling to a compiled query by injecting or capping LIMIT.

    If the query has no LIMIT, adds one.
    If the query has a LIMIT larger than sample_size, caps it.
    If the query has a LIMIT smaller than sample_size, keeps it.

    Returns a new CompiledQuery with modified SQL.
    """
    sql = compiled.sql
    match = _LIMIT_RE.search(sql)

    if match:
        existing_limit = int(match.group(1))
        if existing_limit > sample_size:
            sql = sql[:match.start()] + f"LIMIT {sample_size}" + sql[match.end():]
    else:
        # Inject LIMIT before any trailing semicolons or at end
        sql = sql.rstrip().rstrip(";")
        sql = f"{sql} LIMIT {sample_size}"

    return CompiledQuery(
        sql=sql,
        params=compiled.params,
        root_field=compiled.root_field,
        columns=compiled.columns,
        sources=compiled.sources,
    )
