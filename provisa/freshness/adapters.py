# Copyright (c) 2026 Kenneth Stott
# Canary: 3f2a1c8d-6b4e-4a2f-9d1c-7e5b0a9f8c3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FreshnessSubject adapters (REQ-859).

MV, the API/pg cache, and any other consumer expose their observable refresh
state as a :class:`StateSubject` so the one FreshnessPredicate implementation
(REQ-858) evaluates them all — no per-consumer freshness logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class StateSubject:  # REQ-857, REQ-859
    """A FreshnessSubject built from explicit, already-observed state.

    Consumers that hold their own refresh state (MV status/last_refresh_at, a
    cache row's cached_at) wrap it here instead of implementing the protocol
    method-by-method. ``refreshed_at`` is a unix timestamp (None = never); ``ok``
    is the last-refresh outcome; ``token``/``baseline`` drive PROBE; ``upstream``
    carries transitive handles.
    """

    refreshed_at: float | None
    ok: bool = True
    token: str | None = None
    baseline: str | None = None
    upstream_subjects: tuple = field(default_factory=tuple)

    def last_refresh_at(self) -> float | None:
        return self.refreshed_at

    def last_refresh_ok(self) -> bool:
        return self.ok

    def upstream(self) -> tuple:
        return self.upstream_subjects

    def freshness_token(self) -> str | None:
        return self.token

    def refresh_token(self) -> str | None:
        return self.baseline
