# Copyright (c) 2026 Kenneth Stott
# Canary: 4b9e2d17-6a3c-4f58-8d01-e7c5a9b3f264
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""In-memory view of the persisted node freshness state, for the query path's staleness decision
(REQ-1661, amended 2026-10-01).

``query_residency.ensure_resident`` runs before every statement on every surface. Its decision —
does a source this plan reads need a land first? — is a pure function of each table's persisted
``last_refresh_at`` / ``last_refresh_ok`` and the clock. This view holds those two values as this
process last read or wrote them, so a read whose landed sources are fresh decides from memory and
issues no control-plane statement. A decision that says STALE is never taken from memory: the
caller then takes the land locks and reads the persisted state, exactly as before, and what it
reads replaces the view's copy.

Same posture as ``registered_tables_cache``: the instance lives on the ``state`` object, entries
are keyed by org and schema generation (a rebuild may have recreated a landed table), and a short
backstop lifetime bounds how long a copy is reused when the generation has not moved — that is how
another process's land, or its failed land, is seen here.
"""

# Requirements: REQ-1661, REQ-1266

from __future__ import annotations

import threading
import time
from typing import Any

#: The backstop lifetime of one node's copy, in seconds — the registry caches' own
#: (``registered_tables_cache._DEFAULT_TTL_SECONDS``): past it the persisted state is read again.
BACKSTOP_SECONDS = 5.0

_MAX_GENERATIONS = 8

Generation = tuple[str | None, str, int]


class NodeFreshnessView:
    """Per-state, generation-keyed copies of ``{last_refresh_at, last_refresh_ok}`` per node."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_generation: dict[Generation, dict[str, tuple[dict | None, float]]] = {}

    def _for(self, generation: Generation) -> dict[str, tuple[dict | None, float]]:
        held = self._by_generation.get(generation)
        if held is None:
            if len(self._by_generation) >= _MAX_GENERATIONS:
                # One entry per live org x generation; a superseded generation is never asked
                # for again, so dropping the lot is cheaper than tracking which one is oldest.
                self._by_generation.clear()
            held = self._by_generation[generation] = {}
        return held

    def states(self, generation: Generation, nodes: list[str]) -> dict[str, dict | None] | None:
        """Every node's copy, or None when any of them is unknown or past its backstop lifetime —
        a partial answer cannot decide a source."""
        now = time.monotonic()
        with self._lock:
            held = self._for(generation)
            out: dict[str, dict | None] = {}
            for node in nodes:
                entry = held.get(node)
                if entry is None or entry[1] <= now:
                    return None
                out[node] = entry[0]
            return out

    def read(self, generation: Generation, states: dict[str, dict | None]) -> None:
        """What the control plane just returned for these nodes (None: never landed)."""
        expires = time.monotonic() + BACKSTOP_SECONDS
        with self._lock:
            held = self._for(generation)
            for node, state in states.items():
                held[node] = (state, expires)

    def stamped(self, generation: Generation, nodes: list[str], *, at: float, ok: bool) -> None:
        """This process just stamped these nodes' refresh outcome in the control plane."""
        expires = time.monotonic() + BACKSTOP_SECONDS
        with self._lock:
            held = self._for(generation)
            for node in nodes:
                held[node] = ({"last_refresh_at": at, "last_refresh_ok": ok}, expires)


_ATTR = "_req_1661_node_freshness_view"


def view_for(state: Any) -> NodeFreshnessView:
    """The view bound to this ``state`` instance, created on first use — per instance, never a
    module-global, so two unrelated states (two tests, two processes) share nothing."""
    view = getattr(state, _ATTR, None)
    if view is None:
        view = NodeFreshnessView()
        setattr(state, _ATTR, view)
    return view


def generation_of(state: Any) -> Generation:
    from provisa.core.request_context import current_org

    return (
        current_org.get(None),
        getattr(state, "schema_boot_id", ""),
        getattr(state, "schema_version", 0),
    )
