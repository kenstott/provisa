# Copyright (c) 2026 Kenneth Stott
# Canary: fa31383e-114a-4327-8a2e-018799dd9568
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialized view registry — tracks MV definitions and status (REQ-081)."""

# Requirements: REQ-133, REQ-135, REQ-158, REQ-159, REQ-160, REQ-199, REQ-234, REQ-235

from __future__ import annotations

import logging
import time

from provisa.mv.models import MVDefinition, MVStatus

log = logging.getLogger(__name__)


class MVRegistry:  # REQ-133, REQ-135, REQ-158, REQ-159, REQ-160
    """In-memory registry of materialized view definitions.

    Loaded from config at startup. Status updates are tracked here
    and optionally persisted to PG.

    When tenant_id is set, MV IDs are namespaced as "{tenant_id}:{mv.id}"
    so per-tenant registries can coexist without key collisions.
    Single-tenant mode (tenant_id=None) is unchanged.
    """

    def __init__(self, tenant_id: str | None = None) -> None:
        self._mvs: dict[str, MVDefinition] = {}
        self._tenant_id = tenant_id

    def _key(self, mv_id: str) -> str:
        if self._tenant_id is not None:
            return f"{self._tenant_id}:{mv_id}"
        return mv_id

    def register(self, mv: MVDefinition) -> None:  # REQ-543
        self._mvs[self._key(mv.id)] = mv

    def unregister(self, mv_id: str) -> None:  # REQ-234
        self._mvs.pop(self._key(mv_id), None)

    def get(self, mv_id: str) -> MVDefinition | None:  # REQ-543
        return self._mvs.get(self._key(mv_id))

    def get_fresh(self) -> list[MVDefinition]:  # REQ-199
        """Return enabled MVs that are fresh and within their TTL (for the rewriter).

        REQ-199: an MV whose TTL has elapsed is excluded here, so the query falls back to
        the live source instead of being rewritten onto stale data.
        """
        now = time.time()
        return [mv for mv in self._mvs.values() if mv.enabled and mv.is_fresh_at(now)]

    def get_enabled(self) -> list[MVDefinition]:  # REQ-543
        """Return all enabled MVs (for refresh scheduler)."""
        return [mv for mv in self._mvs.values() if mv.enabled]

    def get_due_for_refresh(self) -> list[MVDefinition]:  # REQ-135, REQ-160
        """Return enabled MVs that are due for refresh."""
        now = time.time()
        result = []
        for mv in self._mvs.values():
            if not mv.enabled or mv.status == MVStatus.REFRESHING:
                continue
            if mv.last_refresh_at is None:
                result.append(mv)
            elif (now - mv.last_refresh_at) >= mv.refresh_interval:
                result.append(mv)
        return result

    def mark_stale(self, table_name: str) -> list[str]:  # REQ-543
        """Mark all MVs referencing a table as stale. Returns list of affected MV IDs."""
        affected = []
        for mv in self._mvs.values():
            if table_name in mv.source_tables and mv.status != MVStatus.DISABLED:
                mv.status = MVStatus.STALE
                affected.append(mv.id)
        return affected

    def mark_refreshing(self, mv_id: str) -> None:  # REQ-543
        mv = self._mvs.get(self._key(mv_id))
        if mv:
            mv.status = MVStatus.REFRESHING

    def mark_refreshed(self, mv_id: str, row_count: int) -> None:  # REQ-543
        mv = self._mvs.get(self._key(mv_id))
        if mv:
            mv.status = MVStatus.FRESH
            mv.last_refresh_at = time.time()
            mv.row_count = row_count
            mv.last_error = None

    def mark_refresh_failed(self, mv_id: str, error: str) -> None:  # REQ-543
        mv = self._mvs.get(self._key(mv_id))
        if mv:
            mv.status = MVStatus.STALE
            mv.last_error = error

    def all(self) -> list[MVDefinition]:  # REQ-543
        return list(self._mvs.values())
