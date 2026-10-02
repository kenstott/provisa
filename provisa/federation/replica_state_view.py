# Copyright (c) 2026 Kenneth Stott
# Canary: 7185050d-3181-4842-a8b2-0a8ba21b8cff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""This process's copy of replica state, so a read of a fresh replica costs no control-plane
read (REQ-1661, REQ-1915, REQ-1920).

``query_residency.ensure_resident`` runs before every statement on every surface. For each
replica the statement reads it must know when the replica was last built. This module holds the
last record this process read for each replica.

The rule that makes a copy safe: a copy can only be OLDER than the truth. A build completes,
never un-completes, so a replica the copy says is fresh for a reader is at least that fresh. A
replica the copy says is stale may have been rebuilt since: the caller re-reads that ONE
record, under the per-replica lock here, before it asks for a build. So the mistake a copy can
make is one extra read, never serving something staler than believed, and a burst of stale
reads of one replica makes one control-plane read and one build request per process.

The copy is derived state: it is never written anywhere and is rebuilt by the reads themselves.
"""

# Requirements: REQ-1661, REQ-1915, REQ-1920

from __future__ import annotations

import threading
import time
from typing import Any

from provisa.core.connection_loop import CrossLoopLock
from provisa.federation.replica_state import ReplicaKey, ReplicaRecord

_Scoped = tuple[str | None, ReplicaKey]


class ReplicaStateView:
    """Per-state copies of each replica's record, keyed by org and replica."""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._records: dict[_Scoped, tuple[ReplicaRecord | None, float]] = {}
        self._locks: dict[_Scoped, CrossLoopLock] = {}

    def known(self, org_id: str | None, key: ReplicaKey) -> tuple[bool, ReplicaRecord | None]:
        """``(True, record)`` when this process has read the replica's record (None: it has no
        row), ``(False, None)`` when it never has."""
        with self._guard:
            held = self._records.get((org_id, key))
            return (False, None) if held is None else (True, held[0])

    def age(self, org_id: str | None, key: ReplicaKey) -> float | None:
        """Seconds since this process last read the replica's record; None when it never has."""
        with self._guard:
            held = self._records.get((org_id, key))
            return None if held is None else time.monotonic() - held[1]

    def read(self, org_id: str | None, key: ReplicaKey, record: ReplicaRecord | None) -> None:
        """What the control plane just returned for this replica."""
        with self._guard:
            self._records[(org_id, key)] = (record, time.monotonic())

    def lock(self, org_id: str | None, key: ReplicaKey) -> CrossLoopLock:
        """The lock one replica's re-read and build request are made under, across the request
        threads of this process."""
        with self._guard:
            return self._locks.setdefault((org_id, key), CrossLoopLock())


_ATTR = "_req_1915_replica_state_view"


def view_for(state: Any) -> ReplicaStateView:
    """The view bound to this ``state`` instance, created on first use — per instance, never a
    module-global, so two unrelated states (two tests, two processes) share nothing."""
    view = getattr(state, _ATTR, None)
    if view is None:
        view = ReplicaStateView()
        setattr(state, _ATTR, view)
    return view
