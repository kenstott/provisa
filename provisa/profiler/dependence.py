# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8f1b46-a27c-4e95-8c03-f6b1e9d4a720
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Dependence between a table's columns (REQ-1934, DEPENDENCE BETWEEN COLUMNS).

Measured after the profile statement -- whose distinct counts decide which columns are categories --
in a few further aggregate statements through the governed pipeline, over the run's sample
(maintainer ruling: the same method and fraction; a block or row-filter sample is an independent
draw of the same size, recorded on the run):

* a statement over the PARENT tables reached through the table's many-to-one relationships, counting
  their text columns' distinct values (only where there is a parent);
* the PAIRS statement: the profiled rows, each parent's columns joined on, every column turned into
  its mid-rank ``u`` in (0, 1) -- a category's value entering as the middle of the slice of 0 to 1
  its share occupies, in value order, never as an arbitrary number -- and into a STATE (a category's
  value, a number's decile of ``u``), then expanded by a pair selector and grouped by the pair's two
  states. It yields every pair's joint counts, the sums its rank correlation is computed from, and
  for a category and a number the sums of the number per category (the correlation ratio);
* the TRIPLES statement: for each own column, its best single parent and the next few, grouped by
  the three states, which yields the mutual information of each two-column parent set.

The dependency network takes, per own column, the parent set of at most two columns with the best
mutual information less a BIC penalty for the set's size, or none.
"""

# Requirements: REQ-1934

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any

from provisa.profiler.statement import ColumnSpec, Sample, _base_sql, _ident, qualified

# REQ-1934: a number enters a joint count as the decile of its rank.
JOINT_BUCKETS = 10
# How many next-best single parents are tried beside the best as a column's second parent.
SECOND_PARENT_CANDIDATES = 3
# How many single parents a column's dependency ranking lists.
RANKED_SINGLES = 5


@dataclass(frozen=True)
class ParentSpec:
    """A parent table reached through a many-to-one relationship of the profiled table."""

    relationship: str
    table: str  # domain.table as pgwire publishes it
    table_id: int
    child_key: str  # the profiled table's column, as published
    parent_key: str  # the parent table's column, as published
    columns: list[ColumnSpec]


@dataclass(frozen=True)
class DepColumn:
    """A column entering the dependence measures. ``name`` is how results name it: an own column's
    published name, or ``<relationship>.<column>`` for a parent's."""

    name: str
    expr: str  # the column in the joined rows: j."c3" (own), j."p0_1" (a parent's)
    kind: str  # number | category
    family: str  # numeric | temporal | text | boolean
    own: bool


def parent_column_name(parent: ParentSpec, column: str) -> str:
    return f"{parent.relationship}.{column}"


def distinct_sql(parent: ParentSpec, columns: list[ColumnSpec]) -> str:
    """The distinct and non-null counts of ``parent``'s text and boolean ``columns``, and its rows:
    ``n<i>`` and ``h<i>`` per column, then ``rows``."""
    parts = ", ".join(
        f"COUNT(DISTINCT p.{_ident(c.name)}) AS {_ident(f'n{i}')}, "
        f"COUNT(p.{_ident(c.name)}) AS {_ident(f'h{i}')}"
        for i, c in enumerate(columns)
    )
    return f"SELECT {parts}, COUNT(*) AS {_ident('rows')} FROM {qualified(parent.table)} p"


def parse_distinct(
    names: list[str], row: tuple, columns: list[ColumnSpec]
) -> dict[str, tuple[int, float]]:
    """``distinct_sql``'s one row as ``{column: (distinct values, share of rows holding one)}``."""
    r = dict(zip(names, row))
    rows = int(r["rows"])
    return {
        c.name: (int(r[f"n{i}"]), int(r[f"h{i}"]) / rows if rows else 0.0)
        for i, c in enumerate(columns)
    }


def choose_columns(
    own: list[tuple[ColumnSpec, int, float]],
    parents: list[tuple[ParentSpec, dict[str, tuple[int, float]]]],
    max_numbers: int,
    max_distinct: int,
    max_categories: int,
) -> list[DepColumn]:
    """The columns entering the measures, in column order, own first: numbers (numeric, temporal)
    up to ``max_numbers`` in column order; categories (text, boolean) with no more than
    ``max_distinct`` distinct values, up to ``max_categories`` of them -- the most frequently held
    kept (the largest share of rows holding a value), then the fewest distinct values, then column
    order. ``own``: each own column with its distinct count and share of rows holding a value;
    ``parents``: each parent with the same of its text and boolean columns."""
    candidates: list[tuple[DepColumn, tuple[int, float] | None]] = []
    for i, (spec, distinct, held) in enumerate(own):
        col = _dep_column(spec.name, f"j.{_ident(f'c{i}')}", spec, True)
        candidates.append((col, (distinct, held)))
    for r, (parent, counts) in enumerate(parents):
        for i, spec in enumerate(parent.columns):
            name = parent_column_name(parent, spec.name)
            col = _dep_column(name, f"j.{_ident(f'p{r}_{i}')}", spec, False)
            candidates.append((col, counts.get(spec.name)))
    numbers = [i for i, (c, _) in enumerate(candidates) if c.kind == "number"][:max_numbers]
    categories = [
        (-counts[1], counts[0], i)
        for i, (c, counts) in enumerate(candidates)
        if c.kind == "category" and counts is not None and 0 < counts[0] <= max_distinct
    ]
    kept = set(numbers) | {i for _, _, i in sorted(categories)[:max_categories]}
    return [c for i, (c, _) in enumerate(candidates) if i in kept]


def _dep_column(name: str, expr: str, spec: ColumnSpec, own: bool) -> DepColumn:
    """``spec`` as a candidate; a family that is neither a number nor a category is ``other``,
    never entered."""
    if spec.family in ("numeric", "temporal"):
        kind = "number"
    elif spec.family in ("text", "boolean"):
        kind = "category"
    else:
        kind = "other"
    return DepColumn(name, expr, kind, spec.family, own)


def _joined(table: str, own: list[ColumnSpec], parents: list[ParentSpec], sample: Sample) -> str:
    """The profiled rows -- the run's sample -- with each parent's columns joined on."""
    base_cols = [f"t.{_ident(c.name)} AS {_ident(f'c{i}')}" for i, c in enumerate(own)]
    base = _base_sql(table, base_cols, sample)
    names = [c.name for c in own]
    joins = ""
    picked = [f"b.{_ident(f'c{i}')}" for i in range(len(own))]
    for r, p in enumerate(parents):
        child = _ident(f"c{names.index(p.child_key)}")
        joins += f" LEFT JOIN {qualified(p.table)} p{r} ON p{r}.{_ident(p.parent_key)} = b.{child}"
        picked += [
            f"p{r}.{_ident(c.name)} AS {_ident(f'p{r}_{i}')}" for i, c in enumerate(p.columns)
        ]
    return f"SELECT {', '.join(picked)} FROM ({base}) b{joins}"


def _ranked(cols: list[DepColumn], joined: str) -> str:
    """Each column as its mid-rank ``u`` in (0, 1), its state and, for a number, its value."""
    sel = []
    for i, c in enumerate(cols):
        e = c.expr
        # Every term DOUBLE PRECISION: an engine's decimal division would round u to the scale of
        # its operands.
        rank = f"CAST(RANK() OVER (PARTITION BY {e} IS NULL ORDER BY {e}) AS DOUBLE PRECISION)"
        ties = f"CAST(COUNT(*) OVER (PARTITION BY {e}) AS DOUBLE PRECISION)"
        held = f"CAST(COUNT({e}) OVER () AS DOUBLE PRECISION)"
        u = f"CASE WHEN {e} IS NULL THEN NULL ELSE ({rank} - 0.5 + ({ties} - 1) / 2) / {held} END"
        sel.append(f"{u} AS {_ident(f'u{i}')}")
        if c.kind == "category":
            sel.append(f"CAST({e} AS TEXT) AS {_ident(f's{i}')}")
        else:
            y = (
                f"EXTRACT(EPOCH FROM {e})"
                if c.family == "temporal"
                else f"CAST({e} AS DOUBLE PRECISION)"
            )
            sel.append(f"{y} AS {_ident(f'y{i}')}")
    return f"SELECT {', '.join(sel)} FROM ({joined}) j"


def _state(cols: list[DepColumn], i: int) -> str:
    if cols[i].kind == "category":
        return f"r.{_ident(f's{i}')}"
    u = f"r.{_ident(f'u{i}')}"
    return f"CAST(CAST(FLOOR({u} * {JOINT_BUCKETS}) AS INTEGER) AS TEXT)"


def _case(selector: list[tuple[int, str]]) -> str:
    return "CASE x.k " + " ".join(f"WHEN {k} THEN {expr}" for k, expr in selector) + " END"


def ordered_pairs(cols: list[DepColumn]) -> list[tuple[int, int]]:
    """Every pair of columns, a category first where the pair has one, so a category and a number
    always carry the number second (its values are summed per category)."""
    out = []
    for a in range(len(cols)):
        for b in range(a + 1, len(cols)):
            if cols[a].kind == "number" and cols[b].kind == "category":
                out.append((b, a))
            else:
                out.append((a, b))
    return out


def pairs_sql(
    table: str,
    own: list[ColumnSpec],
    parents: list[ParentSpec],
    cols: list[DepColumn],
    sample: Sample,
) -> str:
    """The pairs statement (module docstring); ``k`` numbers ``ordered_pairs(cols)`` from 1."""
    pairs = ordered_pairs(cols)
    ranked = _ranked(cols, _joined(table, own, parents, sample))
    ks = list(enumerate(pairs, 1))
    sa = "CASE p.k " + " ".join(f"WHEN {k} THEN {_state(cols, a)}" for k, (a, _) in ks) + " END"
    sb = "CASE p.k " + " ".join(f"WHEN {k} THEN {_state(cols, b)}" for k, (_, b) in ks) + " END"
    ua = "CASE p.k " + " ".join(f"WHEN {k} THEN r.{_ident(f'u{a}')}" for k, (a, _) in ks) + " END"
    ub = "CASE p.k " + " ".join(f"WHEN {k} THEN r.{_ident(f'u{b}')}" for k, (_, b) in ks) + " END"
    numeric_b = [(k, b) for k, (a, b) in ks if cols[b].kind == "number"]
    yb = (
        "CASE p.k " + " ".join(f"WHEN {k} THEN r.{_ident(f'y{b}')}" for k, b in numeric_b) + " END"
        if numeric_b
        else "CAST(NULL AS DOUBLE PRECISION)"
    )
    # Two numbers of one family (two dates, two amounts) are compared row by row, for the
    # ordering constraints a profile proposes (REQ-1934 PROPOSED CONSTRAINTS).
    comparable = [
        (k, a)
        for k, (a, b) in ks
        if cols[a].kind == "number"
        and cols[b].kind == "number"
        and cols[a].family == cols[b].family
    ]
    ya = (
        "CASE p.k " + " ".join(f"WHEN {k} THEN r.{_ident(f'y{a}')}" for k, a in comparable) + " END"
        if comparable
        else "CAST(NULL AS DOUBLE PRECISION)"
    )
    selectors = ", ".join(f"({k})" for k, _ in ks)
    expanded = (
        f"SELECT p.k AS k, {sa} AS sa, {sb} AS sb, {ua} AS ua, {ub} AS ub, {yb} AS yb, "
        f"{ya} AS ya "
        f"FROM ({ranked}) r CROSS JOIN (VALUES {selectors}) AS p(k)"
    )
    both = "x.ua IS NOT NULL AND x.ub IS NOT NULL"
    return (
        "SELECT x.k AS k, x.sa AS sa, x.sb AS sb, COUNT(*) AS n, "
        f"COUNT(CASE WHEN {both} THEN 1 END) AS nb, "
        f"SUM(CASE WHEN {both} THEN x.ua END) AS sum_a, "
        f"SUM(CASE WHEN {both} THEN x.ub END) AS sum_b, "
        f"SUM(CASE WHEN {both} THEN x.ua * x.ua END) AS sum_aa, "
        f"SUM(CASE WHEN {both} THEN x.ub * x.ub END) AS sum_bb, "
        f"SUM(CASE WHEN {both} THEN x.ua * x.ub END) AS sum_ab, "
        "COUNT(x.yb) AS ny, SUM(x.yb) AS sum_y, SUM(x.yb * x.yb) AS sum_yy, "
        "COUNT(x.ya * x.yb) AS n_cmp, COUNT(CASE WHEN x.ya <= x.yb THEN 1 END) AS n_le, "
        "COUNT(CASE WHEN x.ya >= x.yb THEN 1 END) AS n_ge "
        f"FROM ({expanded}) x GROUP BY x.k, x.sa, x.sb"
    )


def triples_sql(
    table: str,
    own: list[ColumnSpec],
    parents: list[ParentSpec],
    cols: list[DepColumn],
    triples: list[tuple[int, int, int]],
    sample: Sample,
) -> str:
    """The triples statement: ``k`` numbers ``triples`` -- (target, parent 1, parent 2) -- from 1."""
    ranked = _ranked(cols, _joined(table, own, parents, sample))
    ks = list(enumerate(triples, 1))

    def pick(slot: int) -> str:
        return (
            "CASE p.k " + " ".join(f"WHEN {k} THEN {_state(cols, t[slot])}" for k, t in ks) + " END"
        )

    selectors = ", ".join(f"({k})" for k, _ in ks)
    return (
        "SELECT x.k AS k, x.st AS st, x.s1 AS s1, x.s2 AS s2, COUNT(*) AS n FROM ("
        f"SELECT p.k AS k, {pick(0)} AS st, {pick(1)} AS s1, {pick(2)} AS s2 "
        f"FROM ({ranked}) r CROSS JOIN (VALUES {selectors}) AS p(k)"
        ") x GROUP BY x.k, x.st, x.s1, x.s2"
    )


# -- reading the statements ---------------------------------------------------------------------


@dataclass
class PairStats:
    a: int
    b: int
    joint: dict[tuple[str | None, str | None], int] = field(default_factory=dict)
    nb: int = 0
    sum_a: float = 0.0
    sum_b: float = 0.0
    sum_aa: float = 0.0
    sum_bb: float = 0.0
    sum_ab: float = 0.0
    # per state of a: (rows with a number b, sum of b, sum of b squared)
    by_a: dict[str | None, tuple[int, float, float]] = field(default_factory=dict)
    # two numbers of one family: rows holding both, rows where a <= b, rows where a >= b
    compared: int = 0
    a_le_b: int = 0
    a_ge_b: int = 0

    @property
    def rows(self) -> int:
        return sum(self.joint.values())


def parse_pairs(
    names: list[str], rows: list[tuple], cols: list[DepColumn]
) -> dict[tuple[int, int], PairStats]:
    pairs = ordered_pairs(cols)
    out = {p: PairStats(*p) for p in pairs}
    for raw in rows:
        r = dict(zip(names, raw))
        s = out[pairs[int(r["k"]) - 1]]
        s.joint[(r["sa"], r["sb"])] = int(r["n"])
        s.nb += int(r["nb"])
        s.compared += int(r["n_cmp"])
        s.a_le_b += int(r["n_le"])
        s.a_ge_b += int(r["n_ge"])
        for f in ("sum_a", "sum_b", "sum_aa", "sum_bb", "sum_ab"):
            if r[f] is not None:
                setattr(s, f, getattr(s, f) + float(r[f]))
        if int(r["ny"]):
            n, sy, syy = s.by_a.get(r["sa"], (0, 0.0, 0.0))
            s.by_a[r["sa"]] = (n + int(r["ny"]), sy + float(r["sum_y"]), syy + float(r["sum_yy"]))
    return out


def parse_triples(
    names: list[str], rows: list[tuple], triples: list[tuple[int, int, int]]
) -> dict[tuple[int, int, int], dict[tuple, int]]:
    out: dict[tuple[int, int, int], dict[tuple, int]] = {t: {} for t in triples}
    for raw in rows:
        r = dict(zip(names, raw))
        out[triples[int(r["k"]) - 1]][(r["st"], r["s1"], r["s2"])] = int(r["n"])
    return out


# -- the measures --------------------------------------------------------------------------------


def entropy(counts: dict[Any, int]) -> float:
    total = sum(counts.values())
    if total == 0:
        return 0.0
    return -sum(n / total * math.log(n / total) for n in counts.values() if n > 0)


def _marginal(joint: dict[tuple, int], positions: tuple[int, ...]) -> dict[tuple, int]:
    out: dict[tuple, int] = {}
    for key, n in joint.items():
        k = tuple(key[p] for p in positions)
        out[k] = out.get(k, 0) + n
    return out


def mutual_information(joint: dict[tuple, int], target: int) -> float:
    """MI, in nats, between position ``target`` of the joint's keys and the other positions."""
    rest = tuple(p for p in range(len(next(iter(joint)))) if p != target)
    return max(
        0.0,
        entropy(_marginal(joint, (target,))) + entropy(_marginal(joint, rest)) - entropy(joint),
    )


def spearman(s: PairStats) -> float | None:
    """Pearson's correlation of the two columns' mid-ranks over the rows holding both."""
    n = s.nb
    if n < 2:
        return None
    va = s.sum_aa - s.sum_a**2 / n
    vb = s.sum_bb - s.sum_b**2 / n
    if va <= 0 or vb <= 0:
        return None
    return (s.sum_ab - s.sum_a * s.sum_b / n) / math.sqrt(va * vb)


def correlation_ratio(s: PairStats) -> float | None:
    """eta squared: the share of the number's spread (variance) its category explains."""
    n = sum(g[0] for g in s.by_a.values())
    if n < 2:
        return None
    total = sum(g[1] for g in s.by_a.values())
    total_sq = sum(g[2] for g in s.by_a.values())
    mean = total / n
    ss_total = total_sq - n * mean * mean
    if ss_total <= 0:
        return None
    ss_between = sum(g[0] * (g[1] / g[0] - mean) ** 2 for g in s.by_a.values() if g[0])
    return min(1.0, max(0.0, ss_between / ss_total))


@dataclass(frozen=True)
class Candidate:
    target: int
    parents: tuple[int, ...]
    mi: float
    uncertainty: float | None
    score: float
    joint: dict[tuple, int]  # (target state, parent states...) -> rows


def _score(mi: float, joint: dict[tuple, int], parents: int) -> float:
    """MI less the BIC penalty of the set's parameters, per row: a second parent is kept only
    where it tells enough more than the first to pay for its states."""
    n = sum(joint.values())
    r_t = len(_marginal(joint, (0,)))
    q = len(_marginal(joint, tuple(range(1, parents + 1))))
    return mi - math.log(n) / (2 * n) * (r_t - 1) * q if n > 1 else 0.0


def _oriented(s: PairStats, target: int) -> dict[tuple, int]:
    """The pair's joint counts keyed (target state, other state)."""
    if s.a == target:
        return dict(s.joint)
    return {(b, a): n for (a, b), n in s.joint.items()}


def single_candidates(
    pairs: dict[tuple[int, int], PairStats], cols: list[DepColumn]
) -> dict[int, list[Candidate]]:
    """Per own column, every other column as its single parent, best MI first."""
    out: dict[int, list[Candidate]] = {}
    for t, col in enumerate(cols):
        if not col.own:
            continue
        found = []
        for (a, b), s in pairs.items():
            if t not in (a, b) or not s.joint:
                continue
            other = b if a == t else a
            joint = _oriented(s, t)
            mi = mutual_information(joint, 0)
            h = entropy(_marginal(joint, (0,)))
            found.append(
                Candidate(t, (other,), mi, mi / h if h > 0 else None, _score(mi, joint, 1), joint)
            )
        out[t] = sorted(found, key=lambda c: (-c.mi, cols[c.parents[0]].name))
    return out


def triples_for(singles: dict[int, list[Candidate]]) -> list[tuple[int, int, int]]:
    """(target, best parent, next-best parent) for the triples statement."""
    out = []
    for t, found in singles.items():
        if not found:
            continue
        best = found[0].parents[0]
        for c in found[1 : 1 + SECOND_PARENT_CANDIDATES]:
            out.append((t, best, c.parents[0]))
    return out


def pair_candidates(
    triples: dict[tuple[int, int, int], dict[tuple, int]],
) -> dict[int, list[Candidate]]:
    out: dict[int, list[Candidate]] = {}
    for (t, p1, p2), joint in triples.items():
        if not joint:
            continue
        mi = mutual_information(joint, 0)
        h = entropy(_marginal(joint, (0,)))
        out.setdefault(t, []).append(
            Candidate(t, (p1, p2), mi, mi / h if h > 0 else None, _score(mi, joint, 2), joint)
        )
    return out


def network(
    singles: dict[int, list[Candidate]], pairs: dict[int, list[Candidate]]
) -> dict[int, Candidate]:
    """Per own column, its parent set: the candidate of best score, kept only where it scores above
    an empty set (0)."""
    out = {}
    for t, found in singles.items():
        options = found[:1] + pairs.get(t, [])
        best = max(options, key=lambda c: c.score, default=None)
        if best is not None and best.score > 0:
            out[t] = best
    return out


# -- result rows ---------------------------------------------------------------------------------


def correlation_rows(pairs: dict[tuple[int, int], PairStats], cols: list[DepColumn]) -> list[dict]:
    out = []
    for (a, b), s in pairs.items():
        names = [cols[a].name, cols[b].name]
        rho = spearman(s)
        if rho is not None:
            out.append(
                {
                    "column_name": names[0],
                    "other_column": names[1],
                    "involved_columns": json.dumps(names),
                    "measure": "spearman",
                    "value": rho,
                    "rows": s.nb,
                }
            )
        if cols[a].kind == "category" and cols[b].kind == "number":
            eta = correlation_ratio(s)
            if eta is not None:
                out.append(
                    {
                        "column_name": names[0],
                        "other_column": names[1],
                        "involved_columns": json.dumps(names),
                        "measure": "correlation_ratio",
                        "value": eta,
                        "rows": sum(g[0] for g in s.by_a.values()),
                    }
                )
    return out


def dependency_rows(
    singles: dict[int, list[Candidate]],
    pairs: dict[int, list[Candidate]],
    chosen: dict[int, Candidate],
    cols: list[DepColumn],
) -> list[dict]:
    out = []
    for t, found in singles.items():
        ranked = sorted(
            found[:RANKED_SINGLES] + pairs.get(t, []),
            key=lambda c: (-c.mi, [cols[p].name for p in c.parents]),
        )
        for rank, c in enumerate(ranked, 1):
            parents = [cols[p].name for p in c.parents]
            out.append(
                {
                    "column_name": cols[t].name,
                    "rank": rank,
                    "parent_1": parents[0],
                    "parent_2": parents[1] if len(parents) > 1 else None,
                    "involved_columns": json.dumps([cols[t].name, *parents]),
                    "mutual_information": c.mi,
                    "uncertainty": c.uncertainty,
                    "score": c.score,
                    "in_network": chosen.get(t) is c,
                }
            )
    return out


def _value_and_bucket(col: DepColumn, state: str | None) -> tuple[str | None, int | None]:
    if col.kind == "category":
        return state, None
    return None, None if state is None else int(state)


def joint_rows(chosen: dict[int, Candidate], cols: list[DepColumn]) -> list[dict]:
    out = []
    for t, c in chosen.items():
        parents = [cols[p] for p in c.parents]
        names = [cols[t].name, *[p.name for p in parents]]
        for key, n in sorted(c.joint.items(), key=lambda kv: (-kv[1], str(kv[0]))):
            tv, tb = _value_and_bucket(cols[t], key[0])
            p1v, p1b = _value_and_bucket(parents[0], key[1])
            p2v, p2b = (None, None) if len(parents) < 2 else _value_and_bucket(parents[1], key[2])
            out.append(
                {
                    "column_name": cols[t].name,
                    "parent_1": parents[0].name,
                    "parent_2": parents[1].name if len(parents) > 1 else None,
                    "involved_columns": json.dumps(names),
                    "target_value": tv,
                    "target_bucket": tb,
                    "parent_1_value": p1v,
                    "parent_1_bucket": p1b,
                    "parent_2_value": p2v,
                    "parent_2_bucket": p2b,
                    "row_count": n,
                }
            )
    return out
