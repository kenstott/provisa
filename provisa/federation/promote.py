# Copyright (c) 2026 Kenneth Stott
# Canary: 9f3a2c71-6b08-4e75-9e12-3c7a0d4f9c11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cost-based VIRTUAL/SCAN -> MATERIALIZED promotion (REQ-826 extension).

federate() resolves strategy from CAPABILITY alone: connector present + source type. But
pushdown is a property of (connector, query-shape), not of a source. A source can be
reachable (VIRTUAL/SCAN) yet, for THIS query, need an operator the connector cannot push
down — the whole relation then crosses the wire and the reduction happens locally. When
that un-pushed scan is large, materializing into the engine's own store (which has full
pushdown) and amortizing the one-time load across the freshness TTL beats re-scanning live.

This module is the pure promotion gate: given the connector's Capability (connector.py),
the query's per-source pushdown DEMAND, and a cardinality Estimate (cardinality.py), decide
whether to promote. Fail-open: an UNKNOWN estimate never promotes — VIRTUAL is always
correct (just possibly slow); we never materialize on a guess (CLAUDE.md: no fallback on a
missing value). Only row-REDUCING operators (predicate, aggregate) drive promotion; a
projection- or join-only gap does not, since it need not shrink the scan.
"""

from __future__ import annotations

from dataclasses import dataclass

from provisa.federation.cardinality import Estimate
from provisa.federation.connector import Capability

# A live scan wider than this many rows, whose reducing operator will NOT push down, is worth
# materializing. Tunable per deployment; a caller may override via should_promote(threshold_rows=).
DEFAULT_PROMOTE_ROW_THRESHOLD = 1_000_000


@dataclass(frozen=True)
class PushdownDemand:  # REQ-826 extension
    """What THIS query needs pushed into a given source to stay efficient.

    Each flag is set when the query applies that operator to the source's rows. Only
    ``predicate`` and ``aggregate`` REDUCE the crossing row count; ``join`` may reduce or
    explode, so it never on its own justifies materialization.
    """

    predicate: bool = False
    aggregate: bool = False
    join: bool = False


def unmet_reducing_pushdown(cap: Capability, demand: PushdownDemand) -> bool:
    """A row-reducing operator the query needs but the connector cannot push down.

    True means the full relation scans over the wire and the reduction runs locally — the
    signal that a live read is potentially wasteful for this query.
    """
    return (demand.predicate and not cap.predicate_pushdown) or (
        demand.aggregate and not cap.aggregate_pushdown
    )


def should_promote(
    cap: Capability,
    demand: PushdownDemand,
    estimate: Estimate,
    *,
    threshold_rows: int = DEFAULT_PROMOTE_ROW_THRESHOLD,
) -> bool:
    """Whether a VIRTUAL/SCAN source should be promoted to MATERIALIZED for this query.

    Two conditions, both required:
    - CAPABILITY GAP — a reducing operator the query needs will not push down
      (``unmet_reducing_pushdown``); if pushdown is complete, live read is efficient, keep it.
    - KNOWN-LARGE — the estimate has a value at/above the threshold. UNKNOWN (value None)
      fails open: never materialize on a guess. An approximate stat is sufficient — this is a
      threshold test, not an exact count.
    """
    if not unmet_reducing_pushdown(cap, demand):
        return False
    return estimate.value is not None and estimate.value >= threshold_rows
