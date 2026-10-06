# Copyright (c) 2026 Kenneth Stott
# Canary: ae99360a-cda8-4228-bc2f-fc670644e8fe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Dependence kept in generation (REQ-1939, DEPENDENCE KEPT; ruling Y1): columns the copula draws
keep their measured rank correlation and each its own distribution; a network target is drawn
from the joint counts given its parents' generated states, a parent table's column included;
a declared distribution joins the copula; a column the generator cannot draw by its network
keeps its own draw and is named. Run on an in-process DuckDB."""

from __future__ import annotations

from dataclasses import replace

import duckdb
import pytest

from provisa.fakes.duckdb_functions import register
from provisa.fakes.kinds import parse
from provisa.synthetic.dependence import Dependence, Joint, Parent, cholesky, pearson
from provisa.synthetic.generate import children_count_sql, generation_sql
from provisa.synthetic.plan import (
    DatasetTable,
    Edge,
    ProfiledColumn,
    ProfiledFanout,
    ProfiledTable,
    plan_tables,
)

_COUNTER = duckdb.connect()


def _col(name: str, family: str = "numeric", **kw) -> ProfiledColumn:
    base = dict(
        physical=name,
        family=family,
        null_count=0,
        distinct_count=5000,
        distinct_ratio=0.9,
        integer_only=False,
        min_value="1",
        sketch=tuple(float(i) for i in range(101)),
        top=(),
        frequencies=(),
        shapes=(),
    )
    base.update(kw)
    return ProfiledColumn(**base)


def _table(dependence, fakes=None, rows=5000) -> DatasetTable:
    return DatasetTable(
        table_id=1,
        name="t",
        pgwire_name="s.t",
        columns=(
            ("id", "integer", True),
            ("a", "double", False),
            ("b", "double", False),
            ("region", "varchar", False),
            ("tier", "varchar", False),
        ),
        scale=1.0,
        fakes=fakes or {},
        profile=ProfiledTable(
            "r1",
            rows,
            rows,
            {
                "id": _col("id"),
                "a": _col("a"),
                "b": _col("b", sketch=tuple(float(1000 + 10 * i) for i in range(101))),
                "region": _col(
                    "region",
                    "text",
                    distinct_count=2,
                    distinct_ratio=0.0004,
                    frequencies=(("east", 600), ("west", 400)),
                    shapes=(("aaaa", 1),),
                ),
                "tier": _col(
                    "tier",
                    "text",
                    distinct_count=2,
                    distinct_ratio=0.0004,
                    frequencies=(("gold", 500), ("silver", 500)),
                    shapes=(("aaaa", 1),),
                ),
            },
            (),
            dependence,
        ),
    )


def _count(plan) -> int:
    return _COUNTER.execute(children_count_sql(plan, "duckdb")).fetchone()[0]


def _unmeasured(t, column):
    raise AssertionError(f"{t.name}.{column} was read from its table")


@pytest.fixture
def con():
    c = duckdb.connect()
    register(c)
    return c


def _rows(con, tables, edges=()):
    planned = plan_tables(
        tables, list(edges), seed=11, names={1: "t", 2: "c"}, count_rows=_count, measure=_unmeasured
    )
    out = {}
    for p in planned:
        res = con.execute(generation_sql(p.plan, "duckdb"))
        names = [d[0] for d in res.description]
        out[p.table.name] = ([dict(zip(names, r)) for r in res.fetchall()], p)
    return out


def _spearman(xs, ys) -> float:
    def ranks(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        for rank, i in enumerate(order):
            r[i] = float(rank)
        return r

    rx, ry = ranks(xs), ranks(ys)
    n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    vx = sum((a - mx) ** 2 for a in rx)
    vy = sum((b - my) ** 2 for b in ry)
    return cov / (vx * vy) ** 0.5


def test_the_copula_keeps_the_measured_rank_correlation_and_each_column_its_distribution(con):
    for rho in (0.9, -0.7):
        rows, _p = _rows(con, [_table(Dependence(spearman={("a", "b"): rho}))])["t"]
        a = [r["a"] for r in rows]
        b = [r["b"] for r in rows]
        assert abs(_spearman(a, b) - rho) < 0.06, (rho, _spearman(a, b))
        assert 0 <= min(a) and max(a) <= 100 and 1000 <= min(b) and max(b) <= 2000
    rows, _p = _rows(con, [_table(None)])["t"]
    assert abs(_spearman([r["a"] for r in rows], [r["b"] for r in rows])) < 0.06


def test_a_network_target_follows_its_parents_states(con):
    dep = Dependence(
        network={"tier": (Parent("region"),), "a": (Parent("region"),)},
        joints={
            "tier": (Joint("gold", ("east",), 600), Joint("silver", ("west",), 400)),
            "a": (Joint(9, ("east",), 600), Joint(0, ("west",), 400)),
        },
    )
    rows, planned = _rows(con, [_table(dep)])["t"]
    assert planned.plan.dependence.targets
    # Undeclared categories generate their own values; each generated region takes one tier, and
    # one decile of a, as the joint counts tie them.
    tiers: dict = {}
    amounts: dict = {}
    for r in rows:
        tiers.setdefault(r["region"], set()).add(r["tier"])
        amounts.setdefault(r["region"], []).append(r["a"])
    assert len(tiers) == 2 and all(len(v) == 1 for v in tiers.values())
    assert len({next(iter(v)) for v in tiers.values()}) == 2
    deciles = sorted((min(v) >= 90, max(v) < 10) for v in amounts.values())
    assert deciles == [(False, True), (True, False)]


def test_a_declared_distribution_joins_the_copula(con):
    dep = Dependence(spearman={("a", "b"): 0.9})
    rows, planned = _rows(con, [_table(dep, fakes={"a": parse("uniform(min=0, max=1)")})])["t"]
    assert "a" in planned.plan.dependence.copula
    a = [r["a"] for r in rows]
    assert all(0 <= v <= 1 for v in a)
    assert _spearman(a, [r["b"] for r in rows]) > 0.8


def test_a_column_whose_parent_is_out_of_reach_keeps_its_own_draw_and_is_named(con):
    dep = Dependence(
        network={"tier": (Parent("nowhere", via="parent_id"),)},
        joints={"tier": (Joint("gold", ("x",), 1),)},
    )
    _rows_, planned = _rows(con, [_table(dep)])["t"]
    assert planned.plan.dependence.kept_own == ("tier",)


def test_a_parent_tables_column_is_read_off_the_joined_parent_row(con):
    child = DatasetTable(
        table_id=2,
        name="c",
        pgwire_name="s.c",
        columns=(("id", "integer", True), ("t_id", "integer", False), ("size", "double", False)),
        scale=1.0,
        profile=ProfiledTable(
            "r2",
            5000,
            5000,
            {"id": _col("id", min_value="1"), "t_id": _col("t_id"), "size": _col("size")},
            (),
            Dependence(
                network={"size": (Parent("tier", via="t_id"),)},
                joints={"size": (Joint(9, ("gold",), 10), Joint(0, ("silver",), 10))},
            ),
        ),
    )
    parent = _table(None, fakes={"tier": parse("categories((gold, silver))")}, rows=500)
    parent = replace(
        parent,
        profile=replace(
            parent.profile,
            fanouts=(ProfiledFanout("s.c", 500, tuple(2.0 for _ in range(101))),),
        ),
    )
    out = _rows(con, [parent, child], [Edge(1, "id", 2, "t_id")])
    tiers = {r["id"]: r["tier"] for r in out["t"][0]}
    for r in out["c"][0]:
        if tiers[r["t_id"]] == "gold":
            assert r["size"] >= 90
        else:
            assert r["size"] < 10


def test_the_copula_factor_mixes_to_the_measured_correlation():
    low, shrink = cholesky([[1.0, pearson(0.5)], [pearson(0.5), 1.0]])
    assert abs(low[1][0] - pearson(0.5)) < 1e-12 and shrink == 0.0
    # Measured pairs that disagree are shrunk by the least that lets them factor, never refused.
    bad = [[1.0, 0.99, -0.99], [0.99, 1.0, 0.99], [-0.99, 0.99, 1.0]]
    low, shrink = cholesky(bad)
    assert low[2][2] > 0 and 0 < shrink < 1
    from provisa.synthetic.dependence import _factor, _shrunk

    assert _factor(_shrunk(bad, shrink - 1e-3)) is None  # any less and it would not factor


def test_the_trino_statement_with_dependence_parses():
    import sqlglot

    dep = Dependence(
        spearman={("a", "b"): 0.5},
        network={"tier": (Parent("region"),)},
        joints={"tier": (Joint("gold", ("east",), 1), Joint("silver", ("west",), 1))},
    )
    planned = plan_tables(
        [_table(dep)], [], seed=1, names={1: "t"}, count_rows=_count, measure=_unmeasured
    )
    sqlglot.parse_one(generation_sql(planned[0].plan, "trino"), read="trino")
