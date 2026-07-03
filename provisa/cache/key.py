# Copyright (c) 2026 Kenneth Stott
# Canary: b64c9be1-34e4-4d11-a658-bdaf66cb9789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cache key generation for query results (REQ-078, REQ-544, REQ-864, REQ-866).

The key is computed from a NORMALIZED form of the governed SQL (REQ-864) plus the
resolved governance identity — role_id and the resolved RLS predicate values
(REQ-866). Two users with different RLS filters get different cache entries; two
cosmetically-different but semantically-identical queries get the SAME entry.

Cacheability is gated separately by ``is_cacheable`` (REQ-866, fail-closed): a
query whose identity is not fully resolved into the key — an empty RLS filter, or
a predicate that depends on unresolved session state (``current_setting``) — MUST
NOT be cached, because a per-session value would otherwise let one persona's rows
serve another. The per-tenant prefix (REQ-595) is applied by the store on top.
"""

from __future__ import annotations

import hashlib
import json

# Session-resolved marker: an RLS predicate containing current_setting(...) is
# evaluated per-session by the database, so its per-identity value is NOT present
# in anything the key can see. Such a query is not safely cacheable.
_UNRESOLVED_MARKER = "current_setting("


def _normalize_sql(sql: str) -> str:  # REQ-864
    """Canonicalize cosmetic SQL variation so semantically-identical queries share a key.

    Collapses whitespace, keyword case, identifier quoting, and commutable AND-predicate
    ordering. Literal and predicate VALUES are preserved unchanged, so two distinct
    personas (e.g. ``tenant_id = 'acme'`` vs ``'beta'``) never collapse onto one key
    (REQ-866). Any normalization the parser cannot handle degrades conservatively to a
    less-normalized (or raw) string — a distinct raw string then yields a distinct key,
    i.e. a cache miss, never a wrong hit.
    """
    import sqlglot
    from sqlglot.errors import SqlglotError
    from sqlglot.optimizer.simplify import simplify

    try:
        return simplify(sqlglot.parse_one(sql)).sql(normalize=True, comments=False)
    except (SqlglotError, RecursionError):
        return sql


def is_cacheable(sql: str, rls_rules: dict[int, str]) -> tuple[bool, str]:  # REQ-866
    """Fail-closed cacheability gate for the query result cache.

    A query is cacheable only when every identity dimension is RESOLVED into the key.
    Returns ``(False, reason)`` when it is not — an empty/whitespace RLS filter
    (unresolved RLS context), or a governed SQL / RLS predicate that depends on
    unresolved session state (``current_setting``). Callers MUST consult this before
    reading or writing the cache and treat a False result as no-cache (REQ-865/866):
    never a silent fallback that could serve another persona's rows.
    """
    if _UNRESOLVED_MARKER in sql.lower():
        return False, "governed SQL depends on unresolved session state (current_setting)"
    for table_id, expr in rls_rules.items():
        if not expr or not expr.strip():
            return False, f"RLS rule for table {table_id} has an empty/unresolved filter"
        if _UNRESOLVED_MARKER in expr.lower():
            return False, f"RLS rule for table {table_id} depends on unresolved session state"
    return True, ""


def cache_key(  # REQ-544, REQ-864, REQ-866
    sql: str,
    params: list,
    role_id: str,
    rls_rules: dict[int, str],
) -> str:
    """Generate a deterministic cache key from the normalized query + security context.

    Args:
        sql: The compiled (governed) SQL string — normalized before hashing (REQ-864).
        params: Positional parameters.
        role_id: The requesting role (partitions persona, masking, and column visibility).
        rls_rules: Active resolved RLS rules (table_id → filter expression) for this role.

    Returns:
        SHA-256 hex digest cache key.

    Callers MUST gate on ``is_cacheable`` first (REQ-866); this function assumes the
    identity is resolved and does not itself decide cacheability.
    """
    key_parts = {
        "sql": _normalize_sql(sql),
        "params": params,
        "role_id": role_id,
        "rls": {str(k): v for k, v in sorted(rls_rules.items())},
    }
    canonical = json.dumps(key_parts, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
