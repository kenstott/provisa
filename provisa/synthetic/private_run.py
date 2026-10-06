# Copyright (c) 2026 Kenneth Stott
# Canary: 192d1c8e-b62e-405c-bc21-dd43de2b1ac2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The profiles a private synthetic dataset is generated from (REQ-1939, DIFFERENTIAL PRIVACY).

Each table's profile is rebuilt from statistics measured under ε (:mod:`private_stats`); the
profile run supplies only the structure -- columns, their families, relationships -- and none of
its recorded values. What a private dataset cannot generate without releasing values from an
unknown domain is refused by name, listing every such column: ``categories()`` taking its values
from the table, ``pattern()``, and a text column with no fake whose values are not few enough to
be drawn as shares of anonymous values (their shapes come from real values).

The statistics, by family, are planned before any is measured, so ε is split over a fixed set;
one that the noise makes unnecessary (a column found to have many values needs no shares) is
still charged.
"""

# Requirements: REQ-1939

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from typing import Any, Callable

from provisa.fakes.kinds import Bool, Categories, Ordered, Pattern, Profile
from provisa.synthetic.plan import (
    DatasetRefused,
    DatasetTable,
    Edge,
    ProfiledColumn,
    ProfiledFanout,
    _fake_of,
    _family_of_type,
    _READS_VALUE,
    _spans_rows,
    sql_type,
)
from provisa.synthetic.privacy import Budget
from provisa.synthetic.private_stats import Measurer, epoch

#: How many values a column with no fake may have to be drawn as shares of anonymous values, and
#: how many hot parents a relationship keeps: public, so no statistic decides it.
PRIVATE_CATEGORY_CAP = 50
#: A text key's public shape: digits only.
_TEXT_KEY_SHAPE = "9" * 10
#: The values of a text column with no fake in a private dataset: anonymous lower-case letters.
_ANONYMOUS_SHAPE = "a" * 8
_INTEGER = ("SMALLINT", "INTEGER", "BIGINT")


@dataclass(frozen=True)
class _Need:
    """What one column of a private dataset is measured for."""

    table: DatasetTable
    column: str
    ir_type: str
    nulls: bool = True
    sketch: bool = False
    distinct: bool = False
    shares: bool = False
    declared: tuple[str, ...] | None = None  # shares over these public values
    difference: str | None = None  # the column a relative fake's difference is measured against
    distinct_count: int | None = None  # noised, once the first stage has measured it


def _needs(tables: list[DatasetTable], edges: list[Edge]) -> list[_Need]:
    ids = {t.table_id for t in tables}
    refused: list[str] = []
    out: list[_Need] = []
    for t in tables:
        fks = {e.child_column for e in edges if e.child_id == t.table_id and e.parent_id in ids}
        for name, ir_type, is_pk in t.columns:
            if is_pk:
                continue
            typ = sql_type(ir_type, t.name, name)
            fam = _family_of_type(typ)
            if name in fks:
                out.append(_Need(t, name, ir_type))
                continue
            rule = t.fakes.get(name)
            fake = _fake_of(t, name, ir_type, False)
            where = f"{t.name}.{name}"
            if isinstance(fake, Categories) and fake.values is None:
                refused.append(f"{where}: declare its values, as categories((a, b, c))")
            elif isinstance(fake, Pattern):
                refused.append(
                    f"{where}: declare a fake other than pattern(), whose shapes come from real "
                    f"values -- a method such as bothify(text='??-##'), or categories((a, b, c))"
                )
            elif isinstance(fake, Categories):
                declared = None if fake.shares is not None else fake.values
                out.append(_Need(t, name, ir_type, shares=declared is not None, declared=declared))
            elif isinstance(fake, Bool):
                out.append(
                    _Need(t, name, ir_type, shares=fake.share is None, declared=("true", "false"))
                )
            elif isinstance(fake, Ordered) and fake.distance is None:
                out.append(_Need(t, name, ir_type, difference=fake.column))
            elif isinstance(fake, Profile) or isinstance(fake, _READS_VALUE):
                if fam in ("numeric", "temporal"):
                    out.append(_Need(t, name, ir_type, sketch=True))
                else:
                    out.append(_Need(t, name, ir_type, distinct=True, shares=True))
            elif fake is not None or (rule is not None and not _spans_rows(rule)):
                out.append(_Need(t, name, ir_type))
            elif fam in ("numeric", "temporal", "boolean"):
                out.append(_Need(t, name, ir_type, sketch=True, distinct=True, shares=True))
            else:
                refused.append(
                    f"{where}: declare a fake or a synthetic rule -- a method such as word() or "
                    f"bothify(text='??-##'), or categories((a, b, c)) naming its values"
                )
    if refused:
        raise DatasetRefused(
            "a private dataset releases no value read from its tables, so each of these columns "
            "must declare what it generates: " + "; ".join(sorted(refused))
        )
    return out


def _counts(
    tables: list[DatasetTable], edges: list[Edge], needs: list[_Need], conditions: int
) -> dict[str, int]:
    """How many statistics of each family ``needs`` measures."""
    ids = {t.table_id for t in tables}
    driving = [e for e in edges if e.child_id in ids and e.parent_id in ids]
    return {
        "row_counts": len(tables),
        "null_counts": len(needs),
        "distinct_counts": sum(n.distinct for n in needs),
        "shares": sum(n.shares for n in needs),
        "sketches": sum(n.sketch or n.difference is not None for n in needs),
        # Each relationship: its parents with children, and their children's distribution; a
        # measured condition's also its parents meeting it.
        "fanouts": 2 * len(driving) + 3 * conditions,
        "hot_keys": len(driving),
    }


def first_stage(
    epsilon: float, tables: list[DatasetTable], edges: list[Edge], conditions: int
) -> tuple[Budget, list[_Need]]:
    """The budget's first stage: the distinct counts, which decide which columns' shares are
    measured at all. Their family takes an even share of ε over every family the dataset could
    measure; the rest of ε is split once the noised counts have decided (:func:`second_stage`).
    Deciding from noised results is post-processing, so the choice costs nothing."""
    needs = _needs(tables, edges)
    counts = _counts(tables, edges, needs, conditions)
    possible = [f for f, n in counts.items() if n > 0]
    budget = Budget(epsilon)
    if counts["distinct_counts"]:
        share = epsilon / len(possible)
        budget.families["distinct_counts"] = share
        budget.per_statistic["distinct_counts"] = share / counts["distinct_counts"]
        budget.statistics["distinct_counts"] = counts["distinct_counts"]
    return budget, needs


def second_stage(
    budget: Budget,
    tables: list[DatasetTable],
    edges: list[Edge],
    needs: list[_Need],
    conditions: int,
) -> list[_Need]:
    """The needs as the noised distinct counts decided them -- a column with more values than
    the cap takes no shares -- and the rest of ε split evenly across the families they measure,
    so the charges sum to ε exactly."""
    decided = [
        n
        if n.distinct_count is None
        or n.distinct_count <= PRIVATE_CATEGORY_CAP
        or n.declared is not None
        else replace(n, shares=False)
        for n in needs
    ]
    counts = _counts(tables, edges, decided, conditions)
    rest = {f: n for f, n in counts.items() if n > 0 and f != "distinct_counts"}
    remaining = budget.epsilon - budget.families.get("distinct_counts", 0.0)
    if rest:
        share = remaining / len(rest)
        for f, n in rest.items():
            budget.families[f] = share
            budget.per_statistic[f] = share / n
            budget.statistics[f] = n
    return decided


def plan_budget(
    epsilon: float, tables: list[DatasetTable], edges: list[Edge], conditions: int
) -> tuple[Budget, list[_Need]]:
    """The two stages with every distinct count taken as small: the most the dataset could
    measure (for checking the split)."""
    budget, needs = first_stage(epsilon, tables, edges, conditions)
    return budget, second_stage(budget, tables, edges, needs, conditions)


def _fanout_sketch(
    positive: tuple[float, ...] | None, parents: int, with_children: int
) -> tuple[float, ...]:
    """Children per parent: zero for the parents with none, the rest drawn from ``positive``."""
    zero = max(0.0, (parents - with_children) / parents) if parents else 1.0
    out = []
    for i in range(101):
        q = i / 100
        if positive is None or q <= zero:
            out.append(0.0)
        else:
            k = (q - zero) / (1 - zero) * 100 if zero < 1 else 100
            lo = int(k)
            hi = min(lo + 1, 100)
            out.append(positive[lo] + (k - lo) * (positive[hi] - positive[lo]))
    return tuple(out)


async def private_tables(
    tables: list[DatasetTable],
    edges: list[Edge],
    *,
    epsilon: float,
    seed: int,
    governed: Any,
    exposed: Callable[[DatasetTable, str], str],
    qualified: Callable[[str], str],
    conditions: tuple = (),
) -> tuple[list[DatasetTable], Budget, Measurer, dict]:
    """Each table with its profile rebuilt from statistics measured under ε; the budget as
    spent; the measurer, for the conditional fan-outs measured with the plan; and the measured
    differences of relative fakes, by (table id, column)."""
    from provisa.profiler.statement import _ident

    measured_conditions = sum(1 for c in conditions if c.count == {"measured": True})
    budget, needs = first_stage(epsilon, tables, edges, measured_conditions)
    m = Measurer(budget, random.Random(f"privacy:{seed}"), governed)
    # The first stage: every distinct count, before anything else is measured.
    staged = []
    for need in needs:
        if need.distinct:
            t = need.table
            col = _ident(exposed(t, need.column))
            d = await m.count(
                "distinct_counts",
                f"SELECT COUNT(DISTINCT x.{col}) FROM {qualified(t.pgwire_name)} x",
                (t.name, need.column),
                weigh=False,  # weighed against the noised rows, once they are counted
            )
            need = replace(need, distinct_count=max(d, 1))
        staged.append(need)
    needs = second_stage(budget, tables, edges, staged, measured_conditions)
    by_table: dict[int, list[_Need]] = {}
    for n in needs:
        by_table.setdefault(n.table.table_id, []).append(n)
    rows: dict[int, int] = {}
    out: dict[int, DatasetTable] = {}
    differences: dict[tuple[int, str], list[float | None] | None] = {}
    for t in sorted(tables, key=lambda t: t.name):
        table = qualified(t.pgwire_name)
        n_rows = await m.count("row_counts", f"SELECT COUNT(*) FROM {table} x", (t.name, None))
        rows[t.table_id] = n_rows
        columns: dict[str, ProfiledColumn] = {}
        for name, ir_type, is_pk in t.columns:
            typ = sql_type(ir_type, t.name, name)
            fam = _family_of_type(typ)
            p = t.profile.columns.get(name)
            family = p.family if p is not None else fam
            base = ProfiledColumn(
                physical=name,
                family=family,
                null_count=0,
                distinct_count=n_rows,
                distinct_ratio=1.0,
                integer_only=typ in _INTEGER,
                min_value="1",  # REQ-1939: a private dataset numbers its keys from 1
                sketch=None,
                top=(),
                frequencies=(),
                # Public shapes: a text key's digits, and the anonymous letters a text column
                # with no fake draws its values as (never a shape of a real value).
                shapes=((_TEXT_KEY_SHAPE if is_pk else _ANONYMOUS_SHAPE, 1),)
                if fam == "text"
                else (),
            )
            columns[name] = base
        for need in by_table.get(t.table_id, []):
            col = _ident(exposed(t, need.column))
            name = need.column
            c = columns[name]
            c = replace(
                c,
                null_count=await m.count(
                    "null_counts",
                    f"SELECT COUNT(*) FROM {table} x WHERE x.{col} IS NULL",
                    (t.name, name),
                    against=n_rows,  # a share of the rows
                ),
            )
            if need.distinct_count is not None:
                d = need.distinct_count
                m.weigh((t.name, name), "distinct_counts", n_rows)
                c = replace(c, distinct_count=d, distinct_ratio=d / n_rows if n_rows else 0.0)
            if need.sketch:
                found = await m.distribution(
                    "sketches",
                    table,
                    epoch(f"x.{col}", c.family),
                    "date" if c.family == "temporal" else "number",
                    (t.name, name),
                )
                c = replace(c, sketch=None if found is None else found[0])
            if need.declared is not None and need.shares:
                counts = await m.value_counts(
                    "shares", table, col, list(need.declared), (t.name, name)
                )
                c = replace(c, frequencies=tuple((v, n) for v, n in counts.items() if n > 0))
            elif need.shares:
                # Only a column the first stage found to have few values is measured for shares.
                counts = await m.sorted_counts(
                    "shares", table, col, PRIVATE_CATEGORY_CAP, (t.name, name)
                )
                c = replace(c, frequencies=tuple((f"v{i}", n) for i, n in enumerate(counts)))
            if need.difference is not None:
                other = _ident(exposed(t, need.difference))
                diff = f"({epoch(f'x.{col}', c.family)} - {epoch(f'x.{other}', c.family)})"
                found = await m.distribution(
                    "sketches",
                    table,
                    diff,
                    "number",
                    (t.name, name),
                    where=f" AND x.{other} IS NOT NULL",
                )
                differences[(t.table_id, name)] = (
                    None if found is None else [found[0][i * 5] for i in range(21)]
                )
            columns[name] = c
        out[t.table_id] = replace(
            t,
            profile=replace(
                t.profile, row_count=n_rows, profiled_rows=n_rows, columns=columns, fanouts=()
            ),
        )
    # Fan-out and hot parents, once every table's rows are counted.
    ids = {t.table_id for t in tables}
    for e in edges:
        if e.parent_id not in ids or e.child_id not in ids:
            continue
        parent, child = out[e.parent_id], out[e.child_id]
        sketch = await fanout(m, parent, child, e, exposed, qualified, rows[e.parent_id])
        out[e.parent_id] = replace(
            parent,
            profile=replace(
                parent.profile,
                fanouts=(
                    *parent.profile.fanouts,
                    ProfiledFanout(child.pgwire_name, rows[e.parent_id], sketch),
                ),
            ),
        )
        fk = _ident(exposed(child, e.child_column))
        top = await m.sorted_counts(
            "hot_keys",
            qualified(child.pgwire_name),
            fk,
            PRIVATE_CATEGORY_CAP,
            (child.name, e.child_column),
        )
        cols = dict(out[e.child_id].profile.columns)
        cols[e.child_column] = replace(
            cols[e.child_column], top=tuple((f"k{i}", n) for i, n in enumerate(top))
        )
        out[e.child_id] = replace(
            out[e.child_id], profile=replace(out[e.child_id].profile, columns=cols)
        )
    return [out[t.table_id] for t in tables], budget, m, differences


async def fanout(
    m: Measurer,
    parent: DatasetTable,
    child: DatasetTable,
    e: Edge,
    exposed: Callable[[DatasetTable, str], str],
    qualified: Callable[[str], str],
    parents: int,
    condition: str | None = None,
) -> tuple[float, ...]:
    """Children per parent of relationship ``e`` under ε -- of the parents meeting ``condition``
    (over the parent row, its columns as the org admin reads them) where given."""
    from provisa.profiler.statement import _ident

    label = (child.name, e.child_column)
    fk = _ident(exposed(child, e.child_column))
    key = _ident(exposed(parent, e.parent_column))
    ptable, ctable = qualified(parent.pgwire_name), qualified(child.pgwire_name)
    within = (
        f" WHERE c.{fk} IN (SELECT x.{key} FROM {ptable} x WHERE {condition})" if condition else ""
    )
    if condition:
        parents = await m.count(
            "fanouts", f"SELECT COUNT(*) FROM {ptable} x WHERE {condition}", label
        )
    per_parent = f"(SELECT c.{fk} AS k, COUNT(*) AS n FROM {ctable} c{within} GROUP BY c.{fk})"
    with_children = await m.count("fanouts", f"SELECT COUNT(*) FROM {per_parent} x", label)
    found = await m.distribution("fanouts", per_parent, "x.n", "count", label, sensitivity=2.0)
    return _fanout_sketch(None if found is None else found[0], parents, with_children)
