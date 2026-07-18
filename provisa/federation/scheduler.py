# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7d1a90-4c62-4f18-9b05-2d6a0e4f1c73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The out-of-band refresh driver for load-protected sources (REQ-1141).

The scheduler is the SOLE writer that pulls a load-protected source; the query path never does
(freshness_gate SCHEDULED). On each tick it walks the registered load-protected (source, table)
entries and, per entry, applies the REQ-1141 gate: window + cadence decide cheaply whether to act,
and only then is the (possibly networked) freshness probe run — an unchanged probe resets the clock
and pulls nothing, a changed probe (or no probe) lands a fresh snapshot.

Everything effectful is an INJECTED SEAM so the driver is unit-testable without a store or clock:
- ``entries()``    → the current load-protected entries with their resolved policy + residency.
- ``probe(entry)`` → the freshness token for an entry (only called when the pre-probe gate passes).
- ``land(entry)``  → land a fresh snapshot; returns the new refresh timestamp (the caller's clock).
- ``now()``        → the wall clock.

The driver holds NO state: last_refresh_at and the stored probe token live on each Entry (sourced
from the materialization store), so a resumed process picks up exactly where it left off.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

from provisa.federation.scheduled_refresh import RefreshPolicy, refresh_gate_pre_probe


@dataclass(frozen=True)
class ScheduledEntry:  # REQ-1141
    """One load-protected (source, table) the scheduler tracks. ``policy`` is the resolved
    RefreshPolicy; ``last_refresh_at`` / ``stored_token`` are the entry's residency state from the
    materialization store (None / None when never loaded)."""

    key: str  # a stable "source_id.schema.table" identifier for logging/dedup
    policy: RefreshPolicy
    last_refresh_at: float | None
    stored_token: str | None


@dataclass(frozen=True)
class RefreshOutcome:  # REQ-1141
    """What the driver did for one entry on a tick."""

    key: str
    action: str  # "landed" | "unchanged" | "skipped"
    new_refresh_at: float | None = None
    new_token: str | None = None


async def refresh_due_entries(
    *,
    entries: Callable[[], Iterable[ScheduledEntry]],
    probe: Callable[[ScheduledEntry], Awaitable[str | None]],
    land: Callable[[ScheduledEntry], Awaitable[float]],
    now: Callable[[], float],
) -> list[RefreshOutcome]:
    """Run one scheduler tick over all load-protected entries (REQ-1141).

    Per entry: apply the cheap window+cadence gate; skip if it fails (no probe run). If the entry's
    policy is probe-capable, run the probe — an unchanged token is a no-op that only resets the
    clock (``unchanged``); a changed token or a non-probing policy lands a fresh snapshot
    (``landed``). Returns the per-entry outcomes for logging/metrics; effects happen through the
    injected ``land``/``probe`` seams only."""
    clock = now()
    outcomes: list[RefreshOutcome] = []
    for entry in entries():
        pol = entry.policy
        if not refresh_gate_pre_probe(
            now=clock,
            window=pol.window,
            last_refresh_at=entry.last_refresh_at,
            cadence=pol.cadence,
        ):
            outcomes.append(RefreshOutcome(entry.key, "skipped"))
            continue

        new_token = entry.stored_token
        if pol.probe_capable:
            token = await probe(entry)
            if token is not None and token == entry.stored_token:
                # Unchanged: publish nothing, but the entry is "checked now" — reset its clock so
                # the cadence measures from this probe, not the last actual land (REQ-1141).
                outcomes.append(
                    RefreshOutcome(entry.key, "unchanged", new_refresh_at=clock, new_token=token)
                )
                continue
            new_token = token  # changed (or no token → degrade to always-land, REQ-847)

        refreshed_at = await land(entry)
        outcomes.append(
            RefreshOutcome(entry.key, "landed", new_refresh_at=refreshed_at, new_token=new_token)
        )
    return outcomes
