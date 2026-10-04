# Copyright (c) 2026 Kenneth Stott
# Canary: b64c9be1-34e4-4d11-a658-bdaf66cb9789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The response cache's key (REQ-078, REQ-544, REQ-864, REQ-866, REQ-1897).

There is one response cache, keyed by :func:`raw_sql_cache_key`: a NORMALIZED form of the
governed SQL (REQ-864), its bound values and the governed role. The governed SQL carries the
resolved row filters and session values inline, so two users with different RLS filters read
different statements and get different entries (REQ-866); two cosmetically-different but
semantically-identical statements get the SAME entry.

Cacheability is gated separately by ``is_cacheable`` (REQ-866, fail-closed): a statement whose
identity is not resolved into its text — a predicate that depends on unresolved session state
(``current_setting``) — MUST NOT be cached, because a per-session value would otherwise let one
persona's rows serve another. The per-tenant prefix (REQ-595) is applied by the store on top.
"""

from __future__ import annotations

import functools
import hashlib
import json

# Session-resolved marker: an RLS predicate containing current_setting(...) is
# evaluated per-session by the database, so its per-identity value is NOT present
# in anything the key can see. Such a query is not safely cacheable.
_UNRESOLVED_MARKER = "current_setting("


# Pure text -> text and on every opted-in request's path: a repeated statement is parsed once
# (REQ-1877). Bounded; an evicted statement is simply normalized again.
@functools.lru_cache(maxsize=4096)
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


def is_cacheable(sql: str) -> tuple[bool, str]:  # REQ-866
    """Fail-closed cacheability gate for the response cache.

    A statement is cacheable only when every identity dimension is RESOLVED into its text.
    Returns ``(False, reason)`` when the governed SQL depends on unresolved session state
    (``current_setting``). Callers MUST consult this before reading or writing the cache and treat
    a False result as no-cache (REQ-865/866): never a silent fallback that could serve another
    persona's rows.
    """
    if _UNRESOLVED_MARKER in sql.lower():
        return False, "governed SQL depends on unresolved session state (current_setting)"
    return True, ""


def raw_sql_cache_key(  # REQ-1897
    sql: str,
    params: list,
    role_id: str,
    *,
    wire_formats: list[int] | None,
    as_of: str | None = None,
) -> str:
    """The key of a plan's cached result — the one response cache's key, whichever surface sent
    the statement (REQ-1897).

    ``wire_formats`` is None for a DECODED entry (rows / Arrow — any surface can serve it) and the
    client's pgwire result format codes for a ``pg_datarows`` entry, whose raw DataRow bytes are
    specific to those codes: a binary-format client must never be replayed text-format bytes.

    The entry holds rows, never a surface's own response shape. The governed ``sql`` already carries the resolved RLS predicates and session values (raw-SQL surfaces have
    no separate rules dict); the bound ``params`` and the ``role_id`` partition it further, and the
    store prefixes the org (REQ-595). ``as_of`` is the request-level as-of time (REQ-1163): it
    is not in the governed text, yet a statement over a bitemporal view reads different rows at
    each one, so a read at one as-of (or at none) never shares an entry with another. Callers
    MUST gate on ``is_cacheable`` first (REQ-866)."""
    return _digest(
        {
            "namespace": "raw_sql_rows",
            "sql": _normalize_sql(sql),
            "params": params,
            "role_id": role_id,
            "wire_formats": wire_formats,
            "as_of": as_of,
        }
    )


def _digest(key_parts: dict) -> str:
    canonical = json.dumps(key_parts, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
