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
from enum import Enum

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


class MaskEval(str, Enum):  # REQ-971
    """Where a column mask (a projection expression in the governed IR) is evaluated."""

    SOURCE = (
        "source"  # pushed down: the source evaluates the mask; raw value never crosses the wire
    )
    STREAM = "stream"  # not pushable: bounded-memory streaming eval (engine-side or per-batch tier)


@dataclass(frozen=True)
class MaskPlan:  # REQ-971
    """The evaluation decision for a mask, plus the reason (a compliance receipt)."""

    eval: MaskEval
    reason: str

    @property
    def confidentiality_fallback(self) -> bool:
        """True when the raw value must transit to the engine/tier because the source cannot
        evaluate the mask — the planner acknowledges this and it is surfaced, never silent."""
        return self.eval is MaskEval.STREAM


def plan_mask_evaluation(cap: Capability, *, can_stream: bool = True) -> MaskPlan:  # REQ-971
    """Decide where a column mask evaluates, from the EXISTING connector capability query.

    A mask is a scalar SQL projection expression carried in the governed IR (REQ-263), so it
    participates in projection pushdown exactly as an RLS predicate participates in predicate
    pushdown. A connector that can evaluate expressions at the source — reported by the existing
    ``Capability.predicate_pushdown`` (a WHERE predicate is itself a scalar expression; no
    mask-specific trait is introduced) — evaluates the mask in place, so the raw unmasked value
    never crosses the wire into the middle tier (SOURCE).

    Where the source cannot evaluate the mask, it MUST be computed in a bounded-memory streaming
    stage (STREAM) — engine-side or per-event/per-batch in the tier (the subscription path's
    ``_mask_row`` over an async event stream). This makes masking correctness independent of
    pushdown capability: a capability gap is never a data leak, and tier memory is bounded to
    O(batch), not O(relation).

    Fail loud (REQ-971): if a mask can neither push down nor stream, refuse — it is FORBIDDEN to
    materialize the full unmasked relation in the tier to mask it (no fetchall-then-mask buffer).
    """
    if cap.predicate_pushdown:
        return MaskPlan(
            MaskEval.SOURCE,
            "connector evaluates expressions at source; mask pushes down in the projection",
        )
    if can_stream:
        return MaskPlan(
            MaskEval.STREAM,
            "mask not pushable at source — bounded-memory streaming evaluation (O(batch))",
        )
    raise ValueError(
        "REQ-971: mask can neither push down to the source nor stream — refusing to buffer "
        "the unmasked relation in the middle tier (no fetchall-then-mask)"
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
