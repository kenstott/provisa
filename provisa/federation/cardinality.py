# Copyright (c) 2026 Kenneth Stott
# Canary: 4d2c9a71-6b08-4e75-9f12-3c7a0d4f9c05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Connector cardinality capability and the cheap-count route (REQ-673, REQ-875).

REQ-673 — ``cardinality(source, table) -> Estimate{value, exact, method}``: a source may
expose a CHEAP volume metric so a caller can size a table without a full scan. Resolution
order: (1) a cheap native statistic when the source has one (GraphQL totalCount, OpenAPI
X-Total-Count, PG reltuples, the engine SHOW STATS, Parquet/Iceberg row-group metadata); (2) an
exact COUNT(*) only when the connector reports counting is cheap; (3) UNKNOWN when sizing is
expensive and no native estimate exists — fail-open, never a hidden full scan. ``exact`` and
``method`` tell a consumer what the number is and what it may be used for. The producer is
SPARSE and OPT-IN — a native count is authored only for expensive-count sources.

REQ-875 — cheap-count route: a bare ``count(*)`` over an unmaterialized source with an EXACT
native count is routed to that native call instead of materializing the full dataset to count
it. Three guards, ALL required and fail-closed: SHAPE (bare count(*), no filter the source
can't honor), EXACTNESS (fires only on an exact estimate), and GOVERNANCE (never bypass
Stage-2 RLS/masking — a native total would over-count a persona's visible subset).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CardinalityMethod(str, Enum):  # REQ-673
    NATIVE_COUNT = "native_count"  # exact — GraphQL totalCount, OpenAPI X-Total-Count
    NATIVE_STAT = "native_stat"  # approximate — PG reltuples, the engine SHOW STATS
    COUNT_STAR = "count_star"  # exact — engine COUNT(*), only when the connector says it's cheap
    UNKNOWN = "unknown"  # sizing is expensive and no native estimate exists


@dataclass(frozen=True)
class Estimate:  # REQ-673
    """A cardinality estimate. ``exact`` distinguishes a true count from a statistic."""

    value: int | None
    exact: bool
    method: CardinalityMethod


UNKNOWN_ESTIMATE = Estimate(value=None, exact=False, method=CardinalityMethod.UNKNOWN)


def resolve_cardinality(
    *,
    native_stat: Estimate | None = None,
    exact_count_cheap: bool = False,
    exact_count: int | None = None,
) -> Estimate:
    """Resolve a table's cardinality by the REQ-673 order, or UNKNOWN (fail-open).

    ``native_stat`` — the source's own estimate (exact totalCount or approximate reltuples),
    or None. ``exact_count_cheap`` — the connector reports a COUNT(*) is cheap; ``exact_count``
    — that count's value when available.
    """
    if native_stat is not None:
        return native_stat
    if exact_count_cheap and exact_count is not None:
        return Estimate(value=exact_count, exact=True, method=CardinalityMethod.COUNT_STAR)
    return UNKNOWN_ESTIMATE


def can_route_cheap_count(
    *,
    is_bare_count_star: bool,
    estimate: Estimate,
    rls_applies: bool,
) -> bool:
    """Whether a count(*) query may be answered by the native count (REQ-875).

    Fail-closed: fires only for a bare count(*) shape, an EXACT estimate with a value, and
    when NO RLS predicate applies to the table for the requesting persona (else a native
    total over-counts the visible subset). Any guard failing → materialize-and-count.
    """
    return is_bare_count_star and estimate.exact and estimate.value is not None and not rls_applies
