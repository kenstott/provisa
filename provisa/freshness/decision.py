# Copyright (c) 2026 Kenneth Stott
# Canary: 9669dd29-7c40-41f3-8de7-0f65722a136a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Freshness decision result (REQ-856).

The freshness module answers one question — given a subject and its observable
state, is it fresh? — and returns fresh / stale / failed plus a human-readable
reason. It is a pure decision: no triggering, no refresh, no side effects. Those
are caller responsibilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Freshness(str, Enum):  # REQ-856
    FRESH = "fresh"
    STALE = "stale"
    FAILED = "failed"


@dataclass(frozen=True)
class Decision:  # REQ-856
    """Outcome of a freshness evaluation."""

    state: Freshness
    reason: str

    @property
    def is_fresh(self) -> bool:
        return self.state is Freshness.FRESH


def fresh(reason: str) -> Decision:
    return Decision(Freshness.FRESH, reason)


def stale(reason: str) -> Decision:
    return Decision(Freshness.STALE, reason)


def failed(reason: str) -> Decision:
    return Decision(Freshness.FAILED, reason)
