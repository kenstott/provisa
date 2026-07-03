# Copyright (c) 2026 Kenneth Stott
# Canary: 6be4860d-8c9d-47d7-8f0f-1bce67ce8939
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FreshnessSubject protocol (REQ-857).

Any entity whose freshness can be evaluated — a materialized view, a pull-through
source, an API response cache, a pgvector index — conforms to this protocol so a
single FreshnessPredicate (REQ-858) can evaluate them all uniformly. The protocol
exposes exactly the observable state a predicate needs; it performs no evaluation
itself.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable


@runtime_checkable
class FreshnessSubject(Protocol):  # REQ-857
    """Observable freshness state of a subject.

    The four capabilities map to REQ-857:
    1. ``last_refresh_at`` — when the subject was last (re)materialized.
    2. ``last_refresh_ok`` — whether that last refresh succeeded.
    3. ``upstream`` — handles to subjects this one derives from (transitive freshness).
    4. ``freshness_token`` — REQ-855's ``freshness_token(source, table)`` dispatch:
       an opaque content token, or ``None`` when the source cannot produce one
       (which degrades PROBE to TTL per REQ-847). ``refresh_token`` is the token
       captured at the last successful refresh, against which PROBE compares.
    """

    def last_refresh_at(self) -> float | None:
        """Unix timestamp of the last refresh, or None if never refreshed."""
        ...

    def last_refresh_ok(self) -> bool:
        """True if the last refresh completed successfully."""
        ...

    def upstream(self) -> Sequence["FreshnessSubject"]:
        """Subjects this one derives from; empty when there are none."""
        ...

    def freshness_token(self) -> str | None:
        """Opaque current content token (REQ-855).

        Returns None when the source cannot produce one — whether structurally
        unsupported or because the transport failed this call. The subject owns
        its I/O and must not raise; a None keeps the predicate pure and degrades
        PROBE to TTL (REQ-847).
        """
        ...

    def refresh_token(self) -> str | None:
        """The freshness_token captured at the last refresh (PROBE baseline)."""
        ...
