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

Also parses GraphQL query comment hints of the form ``# @provisa key=value``:
  - ``# @provisa route=federated`` → force Trino federation
  - ``# @provisa route=direct``    → force direct driver (single-source only)
  - ``# @provisa join=broadcast``  → Trino session: join_distribution_type=BROADCAST
  - ``# @provisa join=partitioned``→ Trino session: join_distribution_type=PARTITIONED
  - ``# @provisa reorder=off``     → Trino session: join_reordering_strategy=NONE
  - ``# @provisa broadcast_size=N``→ Trino session: join_max_broadcast_table_size=N
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


def extract_graphql_comments(query: str) -> list[str]:
    """Extract all ``#`` comment lines from a GraphQL query string.

    Returns each comment text (without the leading ``#``) as a list item,
    in the order they appear.  Both ``@provisa`` directive lines and plain
    description comments are included so callers can choose which to keep.

    Args:
        query: Raw GraphQL query string.

    Returns:
        List of comment strings (stripped), e.g.
        ``["@provisa route=trino", "Fetch all active customers"]``.
    """
    comments: list[str] = []
    for line in query.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            comments.append(stripped[1:].strip())
    return comments


def graphql_comments_to_sql(query: str) -> str:
    """Convert GraphQL ``#`` comment lines to SQL ``--`` comment lines.

    Returns a block of ``-- comment`` lines (terminated with a newline) that
    can be prepended to a SQL string, or an empty string when there are no
    comments.

    Args:
        query: Raw GraphQL query string.

    Returns:
        SQL comment block, e.g. ``"-- @provisa route=trino\\n-- My report\\n"``.
    """
    lines = extract_graphql_comments(query)
    if not lines:
        return ""
    return "".join(f"-- {line}\n" for line in lines)


def graphql_hints_to_session_props(hints: dict[str, str]) -> dict[str, str]:
    """Convert ``# @provisa`` federation hints to Trino session properties.

    Handles the federation-specific keys; routing keys (``route``) are ignored
    here since they are handled by the router, not the executor.

    Args:
        hints: Dict returned by :func:`extract_graphql_hints`.

    Returns:
        Dict of Trino session property name → value, ready to merge into
        ``session_hints`` before execution.
    """
    props: dict[str, str] = {}
    join = hints.get("join", "").lower()
    if join == "broadcast":
        props["join_distribution_type"] = "BROADCAST"
    elif join == "partitioned":
        props["join_distribution_type"] = "PARTITIONED"
    reorder = hints.get("reorder", "").lower()
    if reorder == "off":
        props["join_reordering_strategy"] = "NONE"
    broadcast_size = hints.get("broadcast_size", "")
    if broadcast_size:
        props["join_max_broadcast_table_size"] = broadcast_size
    return props


def extract_graphql_hints(query: str) -> dict[str, str]:
    """Extract ``# @provisa key=value`` hints from GraphQL query comment lines.

    Lines beginning with ``#`` are GraphQL comments; this parser scans for
    the ``@provisa`` marker and collects all ``key=value`` pairs that follow.

    Example::

        # @provisa route=trino
        query MyReport { orders { id } }

    Args:
        query: Raw GraphQL query string.

    Returns:
        Dict of hint key → value, e.g. ``{"route": "trino"}``.
    """
    hints: dict[str, str] = {}
    for line in query.splitlines():
        stripped = line.strip()
        if not stripped.startswith("#"):
            continue
        comment = stripped[1:].strip()
        if not comment.startswith("@provisa"):
            continue
        rest = comment[len("@provisa"):].strip()
        for part in rest.split():
            if "=" in part:
                k, _, v = part.partition("=")
                hints[k.strip()] = v.strip()
    return hints
