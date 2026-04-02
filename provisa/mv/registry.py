# Copyright (c) 2025 Kenneth Stott
# Canary: fa31383e-114a-4327-8a2e-018799dd9568
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialized view registry — tracks MV definitions and status (REQ-081)."""

from __future__ import annotations

import logging
import time

from provisa.mv.models import MVDefinition, MVStatus

log = logging.getLogger(__name__)


class MVRegistry:
    """In-memory registry of materialized view definitions.

    Loaded from config at startup. Status updates are tracked here
    and optionally persisted to PG.
    """

    def __init__(self):
        self._mvs: dict[str, MVDefinition] = {}

    def register(self, mv: MVDefinition) -> None:
        self._mvs[mv.id] = mv

    def get(self, mv_id: str) -> MVDefinition | None:
        return self._mvs.get(mv_id)

    def get_fresh(self) -> list[MVDefinition]:
        """Return all enabled and fresh MVs (for rewriter)."""
        return [
            mv for mv in self._mvs.values()
            if mv.enabled and mv.is_fresh
        ]

    def get_enabled(self) -> list[MVDefinition]:
        """Return all enabled MVs (for refresh scheduler)."""
        return [mv for mv in self._mvs.values() if mv.enabled]

    def get_due_for_refresh(self) -> list[MVDefinition]:
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

    def mark_stale(self, table_name: str) -> list[str]:
        """Mark all MVs referencing a table as stale. Returns list of affected MV IDs."""
        affected = []
        for mv in self._mvs.values():
            if table_name in mv.source_tables and mv.status != MVStatus.DISABLED:
                mv.status = MVStatus.STALE
                affected.append(mv.id)
        return affected

    def mark_refreshing(self, mv_id: str) -> None:
        mv = self._mvs.get(mv_id)
        if mv:
            mv.status = MVStatus.REFRESHING

    def mark_refreshed(self, mv_id: str, row_count: int) -> None:
        mv = self._mvs.get(mv_id)
        if mv:
            mv.status = MVStatus.FRESH
            mv.last_refresh_at = time.time()
            mv.row_count = row_count
            mv.last_error = None

    def mark_refresh_failed(self, mv_id: str, error: str) -> None:
        mv = self._mvs.get(mv_id)
        if mv:
            mv.status = MVStatus.STALE
            mv.last_error = error

    def all(self) -> list[MVDefinition]:
        return list(self._mvs.values())
