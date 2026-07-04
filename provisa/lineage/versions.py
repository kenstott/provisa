# Copyright (c) 2026 Kenneth Stott
# Canary: 9d1e3f5a-7c2b-4048-8a6d-1e3f5a7c9b0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Definition- and input-version stamping for MV refresh traces (REQ-862).

Two version dimensions, both store-independent so they work whether an MV
materializes into Iceberg or a plain RDB target:

- DEFINITION version — a content hash of the MV's own definition (the view SQL /
  join pattern + governing config). Pure Provisa metadata; unrelated to the target.
- INPUT version — the point-in-time of the sources consumed. Captured at the
  strongest fidelity each source offers, degrading gracefully: an Iceberg snapshot
  id, else an RDB watermark epoch (REQ-260), else a REQ-855 freshness token, else
  the refresh wall-clock. The kind is recorded alongside the value so a lineage
  query knows the fidelity.

These land in the refresh span (and the ``mv_refresh_log`` ledger); users derive
point-in-time, column-level lineage by querying those records (REQ-862).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

# Strongest → weakest input-version fidelity.
INPUT_KINDS = ("iceberg_snapshot", "watermark", "freshness_token", "refresh_epoch")
_KIND_RANK = {kind: rank for rank, kind in enumerate(reversed(INPUT_KINDS))}


@dataclass(frozen=True)
class InputVersion:  # REQ-862
    """A single source's version signal, with its fidelity kind."""

    value: str
    kind: str  # one of INPUT_KINDS


def resolve_input_version(signals: list[InputVersion], refresh_epoch: str) -> InputVersion:
    """Pick the strongest input-version signal; fall back to the refresh epoch.

    ``signals`` are the per-source version signals gathered at refresh time (an
    Iceberg snapshot id, a watermark epoch, a freshness token). When none are
    available — the honest case for an RDB source with no watermark — the refresh
    wall-clock epoch is used so the trace always carries an input version.
    """
    usable = [s for s in signals if s.kind in _KIND_RANK and s.value]
    if not usable:
        return InputVersion(refresh_epoch, "refresh_epoch")
    return max(usable, key=lambda s: _KIND_RANK[s.kind])


def mv_definition_version(
    *,
    sql: str | None,
    join_pattern: Any = None,
    source_tables: list[str] | None = None,
    serves_aggregates: bool = False,
    aggregate_columns: list[str] | None = None,
) -> str:
    """Stable content hash of the definition-determining fields of an MV.

    Changes to the view SQL, join pattern, source set, or aggregate config produce a
    new version; cosmetic/runtime fields (last_refresh_at, row_count, status) do not.
    Store-independent — the same value regardless of the materialization target.
    """
    canonical = json.dumps(
        {
            "sql": sql or "",
            "join_pattern": repr(join_pattern) if join_pattern is not None else None,
            "source_tables": sorted(source_tables or []),
            "serves_aggregates": serves_aggregates,
            "aggregate_columns": sorted(aggregate_columns or []),
        },
        sort_keys=True,
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
