# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f2e94-3a7d-4c58-9e02-1d8a4f6c3b27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The engine-agnostic query result type (REQ-028).

Every terminal — the ENGINE terminal (any federation engine) and the DIRECT native driver — returns
a ``QueryResult``. It lives in its own neutral module so generic code never imports it from a
specific engine's executor.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class QueryResult:  # REQ-028
    """Result of executing a SQL query on any engine terminal."""

    rows: list[tuple]
    column_names: list[str]
    column_types: list[str] | None = None
