# Copyright (c) 2026 Kenneth Stott
# Canary: f8a2c1d4-9b3e-4f7a-a6c8-2d5b0e1f4a9c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Federation hint extraction for Trino query optimizer (Phase AL).

Parses SQL comment hints of the form ``/*+ HINT(...) */`` and converts them
to Trino session property key-value pairs.

Supported hints:
  - ``BROADCAST(table)``           → ``join_distribution_type = 'BROADCAST'``
  - ``NO_REORDER``                 → ``join_reordering_strategy = 'NONE'``
  - ``BROADCAST_SIZE(table, N)``   → ``join_max_broadcast_table_size = 'N'``
"""

from __future__ import annotations

import re

# Matches the outer hint block: /*+ ... */
_HINT_BLOCK_RE = re.compile(r"/\*\+\s*(.*?)\s*\*/", re.DOTALL)

# Individual hint token: NAME or NAME(args)
_HINT_TOKEN_RE = re.compile(r"(\w+)(?:\s*\(([^)]*)\))?")


def extract_hints(sql: str) -> tuple[str, dict[str, str]]:
    """Extract optimizer hints from SQL comment blocks.

    Removes the ``/*+ ... */`` hint comment from the SQL string and returns
    both the cleaned SQL and a dict of Trino session properties derived from
    the hints.

    Args:
        sql: SQL string potentially containing ``/*+ HINT */`` comment blocks.

    Returns:
        ``(cleaned_sql, session_props)`` where ``session_props`` maps Trino
        session property names to their string values.
    """
    session_props: dict[str, str] = {}
    cleaned = sql

    for block_match in _HINT_BLOCK_RE.finditer(sql):
        hint_text = block_match.group(1)
        for token_match in _HINT_TOKEN_RE.finditer(hint_text):
            name = token_match.group(1).upper()
            args_raw = token_match.group(2) or ""
            args = [a.strip() for a in args_raw.split(",") if a.strip()]

            if name == "BROADCAST":
                session_props["join_distribution_type"] = "BROADCAST"
            elif name == "NO_REORDER":
                session_props["join_reordering_strategy"] = "NONE"
            elif name == "BROADCAST_SIZE" and len(args) >= 2:
                session_props["join_max_broadcast_table_size"] = args[1]

        # Remove the hint comment from the SQL
        cleaned = cleaned.replace(block_match.group(0), "", 1)

    return cleaned.strip(), session_props
