# Copyright (c) 2026 Kenneth Stott
# Canary: 2c788cb7-ca3f-4947-b57a-58c6bf8df700
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Not too close to a real row (REQ-1939; maintainer rulings W1, Z2, C1).

A dataset declaring a closeness threshold and a number of draws compares every generated row with
a sample of its table's real rows, read as the org admin through the governed pipeline and landed
for the comparison only in the dataset's own store schema -- never registered, so never readable
by the environment's roles, and dropped right after generation, whether it succeeded or failed.

The distance between two rows is the mean, over the columns drawn from the real values, of a
number's difference over the sample's interquartile range, or a category's 0 if equal and 1 if
not. A row whose nearest real row is nearer than the threshold -- its share of the median distance
of a real row to its nearest other -- is drawn again, up to the number of draws, and dropped if
no draw is far enough; a dropped row's children drop with it (C1). The draws redraw the row's
values only: its key, its parent and its children's counts are its structure, the same in every
draw, as are the columns a child's conditions or dependence read.

The report states the threshold, the rows drawn again, dropped and dropped with a parent, the
generated rows' distance to their nearest real row beside the real rows' own, the nearest-neighbour
distance ratio, and the membership test: the chance a generated row lies nearer the real rows than
a real row does to the others (0.5: no nearer).
"""

# Requirements: REQ-1939

from __future__ import annotations

from bisect import bisect_left, bisect_right
from typing import Any

#: The real rows compared with, at most.
SAMPLE_ROWS = 2000
#: The generated rows the report measures, at most.
REPORT_ROWS = 2000
#: The distance quantiles the report states.
REPORT_QUANTILES = (0.05, 0.5)


def quantile(values: list[float], q: float) -> float:
    """PERCENTILE_CONT of values (not empty) at q."""
    v = sorted(values)
    pos = q * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (pos - lo) * (v[hi] - v[lo])


def scale_of(values: list[float]) -> float | None:
    """A number's scale: its interquartile range over the sample; None where it has none (fewer
    than two values, or a range of 0), the column then compared as equal or not."""
    if len(values) < 2:
        return None
    iqr = quantile(values, 0.75) - quantile(values, 0.25)
    return iqr if iqr > 0 else None


def threshold(share: float, real_nearest: list[float]) -> float:
    """The distance a generated row must keep from every real row."""
    return share * quantile(real_nearest, 0.5)


def membership_auc(generated: list[float], real: list[float]) -> float:
    """The chance a generated row's nearest real row is nearer than a real row's nearest other,
    ties counting half: 0.5 when generated rows lie among the real ones as the real ones lie among
    themselves, above it when they lie nearer."""
    r = sorted(real)
    nearer = sum(
        len(r) - bisect_right(r, g) + 0.5 * (bisect_right(r, g) - bisect_left(r, g))
        for g in generated
    )
    return nearer / (len(generated) * len(r))


def ratios(pairs: list[tuple[float, float]]) -> tuple[list[float], int]:
    """Each row's nearest over its second nearest distance; and how many rows have none, their
    second nearest being at distance 0."""
    out = [a / b for a, b in pairs if b > 0]
    return out, len(pairs) - len(out)


def fixed_columns(planned: list[Any]) -> dict[str, frozenset[str]]:
    """By table, the columns every draw gives one value: those a child's conditions or dependence
    read off its parent row -- they decide the child's rows -- and what they are computed from
    (a fake's references; the parent's whole copula and network, which draw together)."""
    from provisa.fakes.projection import references
    from provisa.fakes.sql_subset import read

    out: dict[str, set[str]] = {}
    for p in planned:
        plan = p.plan
        if plan.parent is None:
            continue
        read_cols = {c for k in plan.conditions for c in read(k.condition, group=False).columns}
        dep = plan.dependence
        if dep is not None:
            read_cols |= {r[1] for x in dep.targets for r in x.parents if not isinstance(r, str)}
        out.setdefault(plan.parent.name, set()).update(read_cols)
    by_name = {p.plan.name: p.plan for p in planned}
    for name, cols in out.items():
        plan = by_name[name]
        fakes = {c.name: c.fake for c in plan.columns if c.fake is not None}
        grew = True
        while grew:
            before = len(cols)
            for c in list(cols):
                if c in fakes:
                    cols |= references(fakes[c])
            if plan.dependence is not None and cols & set(plan.dependence.nodes):
                cols |= set(plan.dependence.nodes)
            grew = len(cols) > before
    return {name: frozenset(cols) for name, cols in out.items()}


def _row(table: str, measure: str, source: Any, synthetic: Any, note: str) -> dict:
    return {
        "table_name": table,
        "column_name": None,
        "measure": measure,
        "source_value": source,
        "synthetic_value": synthetic,
        "delta": None if source is None or synthetic is None else synthetic - source,
        "note": note,
    }


def not_checked() -> list[dict]:
    """The report's row for a dataset declaring no closeness (Z2)."""
    return [_row("", "closeness", None, None, "not checked: the dataset declares no threshold")]


def unchecked_table(table: str) -> list[dict]:
    return [
        _row(
            table,
            "closeness",
            None,
            None,
            "not checked: no column of the table is drawn from its real values (each is a key, "
            "a foreign key, or decided by a fake or rule); its rows drop only with a parent",
        )
    ]


def cascaded_only(table: str, cascaded: int) -> list[dict]:
    """The report's rows of a table not checked itself: the rows dropped with a parent."""
    return [
        _row(
            table,
            "closeness_cascaded",
            None,
            float(cascaded),
            "rows dropped because a row they refer to was dropped",
        )
    ]


def report_entries(
    table: str,
    *,
    share: float,
    limit: float,
    draws: int,
    counts: tuple[int, int, int],
    real: list[tuple[float, float]],
    generated: list[tuple[float, float]],
) -> list[dict]:
    """The report's closeness rows of one table. real: each real sample row's nearest and
    second nearest other; generated: each measured generated row's nearest and second nearest
    real row."""
    redrawn, dropped, cascaded = counts
    real_nn = [a for a, _ in real]
    gen_nn = [a for a, _ in generated]
    out = [
        _row(
            table,
            "closeness",
            None,
            None,
            f"checked against {len(real)} real rows: up to {draws} draws per row",
        ),
        _row(
            table,
            "closeness_threshold",
            quantile(real_nn, 0.5),
            limit,
            f"{share!r} of the real rows' median distance to their nearest other (beside)",
        ),
        _row(table, "closeness_redrawn", None, float(redrawn), "rows kept from a later draw"),
        _row(
            table,
            "closeness_dropped",
            None,
            float(dropped),
            "rows dropped: no draw far enough from every real row",
        ),
    ] + cascaded_only(table, cascaded)
    if not gen_nn:
        out.append(_row(table, "closeness_distance", None, None, "no generated row is left"))
        return out
    for q in REPORT_QUANTILES:
        out.append(
            _row(
                table,
                "closeness_distance",
                quantile(real_nn, q),
                quantile(gen_nn, q),
                f"{round(q * 100)}th percentile of the distance to the nearest real row, over "
                f"{len(generated)} generated rows; beside, a real row's to its nearest other",
            )
        )
    real_ratios, real_none = ratios(real)
    gen_ratios, gen_none = ratios(generated)
    out.append(
        _row(
            table,
            "closeness_nndr",
            quantile(real_ratios, 0.5) if real_ratios else None,
            quantile(gen_ratios, 0.5) if gen_ratios else None,
            f"median nearest over second nearest distance (rows with a second nearest at 0, "
            f"left out: {gen_none} generated, {real_none} real)",
        )
    )
    out.append(
        _row(
            table,
            "closeness_membership_auc",
            0.5,
            membership_auc(gen_nn, real_nn),
            "the chance a generated row lies nearer the real rows than a real row lies to the "
            "others (beside: no nearer)",
        )
    )
    return out
