# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2d9a40-6b18-4e75-8f02-1c7a0d4f9b99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Read-through / write-back discipline for pulled-through relations (REQ-847).

Two hard rules for a materialization_store entry served by pull-through:

READ  — on a pull-through read, a pull failure with NO fresh cached data is a HARD ERROR.
        Serving stale data is permitted ONLY under an explicit per-source freshness policy
        (REQ-855), never as a silent fallback.

WRITE — mutations on a pulled-through relation target the UPSTREAM source of truth and
        invalidate the cache entry; the cache is never written as if it were the system of
        record.

Pure policy: given the pull result and cache state, ``resolve_read`` picks the outcome, and
``plan_mutation`` states where a write goes and what it invalidates. The I/O (the pull, the
serve, the upstream write) is carried out by the caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ReadOutcome(str, Enum):  # REQ-847
    SERVE_FRESH = "serve_fresh"  # fresh rows (re-pulled, or cache still within freshness)
    SERVE_STALE = "serve_stale"  # stale rows, permitted only by explicit per-source policy
    HARD_ERROR = "hard_error"  # pull failed and no fresh data — never a silent stale fallback


def resolve_read(
    *,
    pull_ok: bool,
    cache_fresh: bool,
    cache_has_data: bool,
    stale_policy_allows: bool,
) -> ReadOutcome:
    """Decide how to satisfy a pull-through read (REQ-847).

    ``pull_ok`` — a re-pull succeeded (rows are current). ``cache_fresh`` — existing cache is
    within its freshness gate (REQ-855). ``cache_has_data`` — any cached rows exist.
    ``stale_policy_allows`` — the source's explicit policy permits serving stale on pull failure.
    """
    if pull_ok or cache_fresh:
        return ReadOutcome.SERVE_FRESH
    if cache_has_data and stale_policy_allows:
        return ReadOutcome.SERVE_STALE
    return ReadOutcome.HARD_ERROR  # pull failed, nothing fresh, no explicit stale allowance


@dataclass(frozen=True)
class MutationPlan:  # REQ-847
    """A write on a pulled-through relation: it goes upstream and invalidates the cache."""

    target: str  # always "upstream" — the source of truth, never the cache
    invalidate: str  # the cache entry (relation) to drop


def plan_mutation(relation_id: str) -> MutationPlan:
    """A mutation on a pulled-through relation targets upstream and invalidates the entry (REQ-847)."""
    return MutationPlan(target="upstream", invalidate=relation_id)
