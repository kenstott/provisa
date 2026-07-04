# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2c9d40-4b18-4e75-8f02-1c7a0d4f9b86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one centralized materialization_store freshness gate (REQ-855).

Per cache entry (per-table, keyed like the replica/view targets) the steward selects one
of three freshness modes, evaluated lazily on access:

- TTL       — bounded staleness; fresh while within the refresh interval.
- PROBE     — re-probe the upstream on access and rebuild only on change, ignoring TTL.
- TTL_PROBE — probe only after a TTL floor elapses, capping probe frequency.

The probe returns an OPAQUE token; Provisa never interprets it, only compares stored vs.
fresh by equality: unchanged → keep the materialized rows (zero lag at probe cost),
changed → invalidate and re-pull, then store the new token. A probe returning None means
the source cannot produce a token, so the entry degrades to TTL — a capability signal,
never a silent stale fallback (REQ-847).

This module is the pure gate: mode selection + token comparison + degrade. The per-source
probe transports (graphql query, openapi ETag/Last-Modified, DB-SQL steward query, file
mtime) implement ``freshness_token(source, table) -> str | None`` and plug in as ``probe``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class FreshnessMode(str, Enum):  # REQ-855
    TTL = "ttl"
    PROBE = "probe"
    TTL_PROBE = "ttl_probe"


@dataclass(frozen=True)
class FreshnessDecision:  # REQ-855
    """Result of the gate. ``fresh`` = keep the materialized rows (skip re-pull/rebuild).

    ``new_token`` carries the freshly probed token to persist (equal to the stored one when
    unchanged, the new value when changed); None when no probe ran or the source has no token.
    """

    fresh: bool
    new_token: str | None = None


# A probe yields an opaque token, or None when the source cannot produce one.
FreshnessProbe = Callable[[], "str | None"]


def evaluate_freshness(
    mode: FreshnessMode,
    *,
    now: float,
    last_refresh_at: float | None,
    ttl: float,
    stored_token: str | None,
    probe: FreshnessProbe | None,
) -> FreshnessDecision:
    """Evaluate the freshness gate for one materialization_store entry (REQ-855)."""
    ttl_elapsed = last_refresh_at is None or (now - last_refresh_at) >= ttl

    if mode is FreshnessMode.TTL:
        return FreshnessDecision(fresh=not ttl_elapsed)

    # TTL_PROBE caps probe frequency: within the TTL floor it is fresh without probing.
    if mode is FreshnessMode.TTL_PROBE and not ttl_elapsed:
        return FreshnessDecision(fresh=True)

    # PROBE (always), or TTL_PROBE past its floor: probe the upstream.
    token = probe() if probe is not None else None
    if token is None:
        # No token capability → degrade to TTL (capability signal, not a silent fallback).
        return FreshnessDecision(fresh=not ttl_elapsed)
    if token == stored_token:
        return FreshnessDecision(fresh=True, new_token=token)  # unchanged → keep, reset clock
    return FreshnessDecision(fresh=False, new_token=token)  # changed → invalidate + re-pull
