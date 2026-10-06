# Copyright (c) 2026 Kenneth Stott
# Canary: 9e1b4d70-2a83-4c6f-b5e2-d8f3a1c07b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How close a generated table came to the profile it was drawn from (REQ-1939).

One row per comparison: row counts, each column's null share, the Kolmogorov-Smirnov distance
between its source and synthetic quantile sketches, the share of its most frequent value (skew),
the KS distance between each relationship's children-per-parent sketches, and a note for every
column generated without declared fake attributes or without a profile.
"""

from __future__ import annotations

from typing import Any

from provisa.profiler.statement import ProfileAggregates
from provisa.synthetic.plan import PlannedTable


def ks_between(
    a: tuple[float, ...] | list[float], b: tuple[float, ...] | list[float], measures: Any
) -> float:
    """The largest gap between the two sketches' distributions, over both sketches' points."""
    points = sorted(set(a) | set(b))
    return max(
        abs(measures.sketch_cdf(list(a), x) - measures.sketch_cdf(list(b), x)) for x in points
    )


def _row(
    table: str,
    column: str | None,
    measure: str,
    src: float | None,
    syn: float | None,
    delta: float | None,
    note: str | None = None,
) -> dict:
    return {
        "table_name": table,
        "column_name": column,
        "measure": measure,
        "source_value": src,
        "synthetic_value": syn,
        "delta": delta,
        "note": note,
    }


def compare(planned: PlannedTable, synthetic: ProfileAggregates, measures: Any) -> list[dict]:
    t = planned.table
    src = t.profile
    name = t.name
    out = []
    generated_by_relationship = planned.plan.fanout is not None
    out.append(
        _row(
            name,
            None,
            "row_count",
            float(src.row_count),
            float(synthetic.profiled_rows),
            synthetic.profiled_rows / src.row_count if src.row_count else None,
            (
                "rows follow the parents' children-per-parent distribution"
                if generated_by_relationship
                else f"expected ratio {t.scale}"
            ),
        )
    )
    syn_cols = {c.spec.physical: c for c in synthetic.columns}
    for physical, prof in src.columns.items():
        syn = syn_cols.get(physical)
        if syn is None:
            continue
        src_null = prof.null_count / src.profiled_rows if src.profiled_rows else None
        syn_rows = synthetic.profiled_rows
        syn_null = (syn_rows - syn.non_null) / syn_rows if syn_rows else None
        out.append(
            _row(
                name,
                physical,
                "null_share",
                src_null,
                syn_null,
                None if src_null is None or syn_null is None else abs(syn_null - src_null),
            )
        )
        if prof.sketch is not None and syn.quantiles is not None:
            out.append(
                _row(
                    name,
                    physical,
                    "ks",
                    None,
                    None,
                    ks_between(prof.sketch, syn.quantiles, measures),
                )
            )
        top = next((n for v, n in prof.top if v is not None), None)
        syn_top = next((n for v, n in syn.values if v is not None), None)
        src_nn = src.profiled_rows - prof.null_count
        if top is not None and syn_top is not None and src_nn and syn.non_null:
            out.append(
                _row(
                    name,
                    physical,
                    "top_share",
                    top / src_nn,
                    syn_top / syn.non_null,
                    abs(syn_top / syn.non_null - top / src_nn),
                )
            )
    syn_fans = {f.spec.child_table: f for f in synthetic.fanouts}
    for fan in src.fanouts:
        syn = syn_fans.get(fan.child_table)
        if syn is None or syn.quantiles is None:
            continue
        out.append(
            _row(
                name,
                None,
                "fanout_ks",
                None,
                None,
                ks_between(fan.sketch, syn.quantiles, measures),
                f"children per parent in {fan.child_table}",
            )
        )
    for column in planned.undeclared:
        out.append(
            _row(
                name, column, "undeclared_fake", None, None, None, "generated from its own profile"
            )
        )
    for column in planned.unprofiled:
        out.append(
            _row(
                name,
                column,
                "unprofiled",
                None,
                None,
                None,
                "not in the profile run: generated NULL",
            )
        )
    return out
