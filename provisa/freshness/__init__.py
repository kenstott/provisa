# Copyright (c) 2026 Kenneth Stott
# Canary: bcebf4e2-39c2-46fb-b7b7-1ec7fbd5b94c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Standalone, context-agnostic freshness module (REQ-856, REQ-857, REQ-858).

Answers one question — given a subject and its observable state, is it fresh? —
and returns fresh / stale / failed with a reason. Pure decision logic: no
triggering or refresh (caller responsibilities). MV, Source, cache, and pgvector
all consume this one implementation via the FreshnessSubject protocol.
"""

from provisa.freshness.decision import Decision, Freshness
from provisa.freshness.predicate import (
    Probe,
    Strategy,
    Transitive,
    Ttl,
    TtlThenProbe,
    evaluate,
)
from provisa.freshness.subject import FreshnessSubject

__all__ = [
    "Decision",
    "Freshness",
    "FreshnessSubject",
    "Strategy",
    "Ttl",
    "Probe",
    "Transitive",
    "TtlThenProbe",
    "evaluate",
]
