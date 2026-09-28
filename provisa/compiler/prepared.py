# Copyright (c) 2026 Kenneth Stott
# Canary: 166d371b-c4d7-4d86-a187-444fa2498949
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prepared-statement-style cache for the raw-SQL pipeline's pure pre-governance stage.

REQ-1866: a raw-SQL surface (pgwire today; Bolt/HTTP-Cypher/Flight SQL are follow-ups) re-parses
and re-normalizes caller-authored SQL text on every single call, even a byte-identical repeat,
while GraphQL's resolver never pays this cost at all (it emits already-semantic SQL directly from
its schema, compiled once at boot). This module caches exactly that gap and nothing else.

SCOPE, deliberately narrow: this cache covers ONLY parsing, REQ-1159's inline-command
localization *check*, and REQ-1317's metric-expansion — the work `_govern_and_route_planned`
(provisa/pgwire/_pipeline.py) does before it ever calls into governance. It does NOT touch, cache,
or shortcut `build_governance_context`/`validate_sql`/`apply_governance`/`decide_route` (masking,
RLS, relationship-guard, or routing) — those already run once per call for every surface,
including GraphQL, and are confirmed cheap (GraphQL's own ~17ms total includes them); there is no
performance case for touching them, only risk. They keep running in full, on every call,
unconditionally, whether this cache hits or misses.

NOT CACHEABLE, and deliberately excluded: a statement that composes a registered command inline
(`_localize_inline_commands` returns True) invokes that command with THIS call's literal
arguments and bakes the result into the rewritten tree — the output is per-call by construction,
never reusable across calls even for the identical SQL shape. Such a statement is simply never
cached; the caller falls through to running the original, uncached code path exactly as before
this module existed.

Invalidation: keyed on `(state.schema_boot_id, state.schema_version)`, the same generation pair
`_rebuild_schemas_impl` already bumps on every schema/masking/RLS/relationship/tracked-function
change (see REQ-1866's own note on the pre-existing RLS/role-mutation rebuild gap, fixed
separately) — a schema/config change invalidates every cached entry for every role at once.
"""

# Requirements: REQ-1866

from __future__ import annotations

import hashlib
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import sqlglot

# Bounded so a pathological caller issuing endless distinct one-off SQL texts cannot grow this
# without limit — an ordinary LRU-by-insertion-order eviction (not a strict LRU-by-access) is
# enough here: this is a pure speed optimization, never a correctness dependency, so evicting the
# "wrong" entry only costs a future cache miss, never a wrong answer.
_MAX_ENTRIES = 4096

_lock = threading.Lock()
# key -> (generation, normalized_sql_text, metric_semantic_sql)
_cache: dict[str, tuple[tuple[str, int], str, str | None]] = {}


@dataclass(frozen=True)
class PreparedFrontEnd:
    """The result of the cacheable pre-governance stage: an already-parsed, already-localized,
    already-metric-expanded statement, ready to hand to governance exactly as
    `_govern_and_route_planned` would have produced it uncached."""

    normalized_sql: str
    parsed: Any  # a sqlglot exp.Expression; typed loosely to sidestep sqlglot's own Expr/
    # Expression stub mismatch (isinstance-true at runtime; pyright disagrees statically)
    cache_hit: bool
    # REQ-1322: None unless a metrics.<name> reference was expanded this call — carried through
    # a cache hit too, so the explain surface reports the same form it would have uncached.
    metric_semantic_sql: str | None = None


def _cache_key(raw_sql: str, role_id: str, generation: tuple[str, int]) -> str:
    # SHA-256, not the raw text, as the dict key: raw_sql is caller-authored and unbounded in
    # length; this keeps the key itself small and fixed-size regardless of statement size.
    digest = hashlib.sha256(f"{role_id}\x00{raw_sql}".encode()).hexdigest()
    return f"{generation[0]}\x00{generation[1]}\x00{digest}"


def clear() -> None:
    """Drop every cached entry. Exposed for tests; production invalidation is generation-keyed
    (a stale generation's entries simply stop matching, see module docstring) so this is never
    called from request-serving code."""
    with _lock:
        _cache.clear()


async def prepare_front_end(
    raw_sql: str,
    role_id: str,
    state: object,  # object-ok: AppState lives in provisa.api, which this compiler-layer module
    # must not import (the exact layering violation this parameter's injection avoids below) —
    # only ever duck-typed via getattr, never a concrete attribute access on `state` itself.
    localize_inline_commands: Callable[[Any, str, object], Awaitable[bool]],
) -> PreparedFrontEnd:
    """Parse + localize-check + metric-expand `raw_sql` for `role_id`, using the cache when a
    byte-identical statement for the same role has already gone through this stage under the
    current schema generation, and the earlier call found no inline command to localize.

    Mirrors `_govern_and_route_planned`'s own pre-governance block exactly (same functions, same
    order, same behavior on a miss) — a caller can substitute this for that block with no
    observable difference beyond a cache hit skipping redundant parse/localize-check/expand work.

    `localize_inline_commands` is injected (REQ-1159's `_localize_inline_commands`, always) rather
    than imported directly: `provisa/compiler/` is a lower layer `pgwire`/`executor`/`api` depend
    on, never the reverse (enforced by this repo's import-linter contracts) — `_pipeline.py` lives
    above this module, so this module cannot import it, only accept it as a parameter from a
    caller that already can.
    """
    generation = (
        getattr(state, "schema_boot_id", ""),
        getattr(state, "schema_version", 0),
    )
    key = _cache_key(raw_sql, role_id, generation)

    with _lock:
        cached = _cache.get(key)
    if cached is not None and cached[0] == generation:
        parsed = sqlglot.parse_one(cached[1], read="postgres")
        return PreparedFrontEnd(
            normalized_sql=cached[1],
            parsed=parsed,
            cache_hit=True,
            metric_semantic_sql=cached[2],
        )

    parsed_input = sqlglot.parse_one(raw_sql, read="postgres")
    normalized_sql = raw_sql

    localized = await localize_inline_commands(parsed_input, role_id, state)
    if localized:
        # Per-call by construction (baked-in literal command-invocation result) — never cache
        # this statement, this call or any future one. Behave exactly as the uncached path would.
        normalized_sql = parsed_input.sql(dialect="postgres")
        return PreparedFrontEnd(normalized_sql=normalized_sql, parsed=parsed_input, cache_hit=False)

    from provisa.compiler.metric_expand import expand_metric_query

    metric_registry = getattr(state, "metrics", {})
    metric_semantic_sql: str | None = None
    if metric_registry:
        metric_tables = {
            t["table_name"]: {
                "id": t["id"],
                "columns": [c["column_name"] for c in t.get("columns", [])],
            }
            for t in getattr(state, "tables", [])
        }
        expanded = expand_metric_query(
            parsed_input, metric_registry, metric_tables, getattr(state, "relationships", [])
        )
        if expanded is not None:
            parsed_input = expanded
            normalized_sql = parsed_input.sql(dialect="postgres")
            metric_semantic_sql = normalized_sql
            from provisa.observability.stage_trace import trace_stage

            trace_stage("metric.expand", normalized_sql)

    with _lock:
        if len(_cache) >= _MAX_ENTRIES:
            _cache.pop(next(iter(_cache)))
        _cache[key] = (generation, normalized_sql, metric_semantic_sql)

    return PreparedFrontEnd(
        normalized_sql=normalized_sql,
        parsed=parsed_input,
        cache_hit=False,
        metric_semantic_sql=metric_semantic_sql,
    )
