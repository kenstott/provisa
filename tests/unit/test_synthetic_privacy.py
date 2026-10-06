# Copyright (c) 2026 Kenneth Stott
# Canary: 502ae642-ac39-4402-8929-c62ef42a0028
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Differential privacy for a synthetic dataset (REQ-1939, DIFFERENTIAL PRIVACY; maintainer
rulings X1 and the P9a audit): every statistic is measured from the table under ε by a mechanism
of bounded sensitivity -- nothing is read from the profile run's recorded values; a distribution's
bounds come from a histogram over a public grid, its values clipped to them; ε is split evenly
across the families present and stated in the report; what would release values from an unknown
domain is refused by name. Measured against an in-process DuckDB table."""

from __future__ import annotations

import asyncio

import duckdb
import pytest
import sqlglot

from provisa.fakes.kinds import parse
from provisa.synthetic.plan import (
    DatasetRefused,
    DatasetTable,
    Edge,
    ProfiledColumn,
    ProfiledFanout,
    ProfiledTable,
)
from provisa.synthetic.privacy import budget_for, report_entries
from provisa.synthetic.private_run import plan_budget, private_tables
from provisa.synthetic.private_stats import grid

# What the profile run recorded: values that must never reach a private dataset.
_RECORDED = 424242.0


def _recorded(name: str, family: str) -> ProfiledColumn:
    return ProfiledColumn(
        physical=name,
        family=family,
        null_count=999,
        distinct_count=7,
        distinct_ratio=0.007,
        integer_only=False,
        min_value=str(_RECORDED),
        sketch=tuple(_RECORDED for _ in range(101)),
        top=(("secret-key", 999),),
        frequencies=(("secret-value", 999),),
        shapes=(("SECRET-99", 999),),
    )


def _con():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA s")
    con.execute("CREATE TABLE s.people (id INTEGER, salary DOUBLE, tier VARCHAR, active BOOLEAN)")
    rows = [(i, 1000.0 + i, ("gold", "silver")[i % 2], i % 4 == 0) for i in range(1, 2001)]
    rows.append((2001, 1e9, "gold", True))  # one person's salary: no bound may reveal it
    con.executemany("INSERT INTO s.people VALUES (?, ?, ?, ?)", rows)
    con.execute("CREATE TABLE s.orders (id INTEGER, person_id INTEGER)")
    con.executemany(
        "INSERT INTO s.orders VALUES (?, ?)", [(i, 1 + i % 500) for i in range(1, 3001)]
    )
    return con


def _governed(con):
    async def run(sql: str):
        duck = sqlglot.transpile(sql, read="postgres", write="duckdb")[0]
        res = con.execute(duck)
        return [d[0] for d in res.description], res.fetchall()

    return run


def _tables(fakes=None) -> list[DatasetTable]:
    people = DatasetTable(
        table_id=1,
        name="people",
        pgwire_name="s.people",
        columns=(
            ("id", "integer", True),
            ("salary", "double", False),
            ("tier", "varchar", False),
            ("active", "boolean", False),
        ),
        scale=1.0,
        # A text column must declare what it generates in a private dataset.
        fakes={"tier": parse("categories((gold, silver))")} if fakes is None else fakes,
        profile=ProfiledTable(
            "r1",
            7,
            7,
            {
                "id": _recorded("id", "numeric"),
                "salary": _recorded("salary", "numeric"),
                "tier": _recorded("tier", "text"),
                "active": _recorded("active", "boolean"),
            },
            (ProfiledFanout("s.orders", 7, tuple(_RECORDED for _ in range(101))),),
        ),
    )
    orders = DatasetTable(
        table_id=2,
        name="orders",
        pgwire_name="s.orders",
        columns=(("id", "integer", True), ("person_id", "integer", False)),
        scale=1.0,
        profile=ProfiledTable(
            "r2",
            7,
            7,
            {"id": _recorded("id", "numeric"), "person_id": _recorded("person_id", "numeric")},
            (),
        ),
    )
    return [people, orders]


_EDGES = [Edge(1, "id", 2, "person_id", "people-orders")]


def _private(tables, seed=1, epsilon=50.0, con=None):
    return asyncio.run(
        private_tables(
            tables,
            _EDGES,
            epsilon=epsilon,
            seed=seed,
            governed=_governed(con or _con()),
            exposed=lambda t, c: c,
            qualified=lambda name: name,
        )
    )


def test_epsilon_is_split_evenly_across_the_families_present():
    b = budget_for(2.0, {"row_counts": 2, "sketches": 1, "shares": 0})
    assert b.families == {"row_counts": 1.0, "sketches": 1.0}
    assert b.per_statistic == {"row_counts": 0.5, "sketches": 1.0}
    budget, _needs = plan_budget(1.0, _tables(), _EDGES, 0)
    assert abs(sum(budget.families.values()) - 1.0) < 1e-12
    with pytest.raises(ValueError, match="ε must be above 0"):
        budget_for(0, {"row_counts": 1})


def test_nothing_the_profile_recorded_reaches_a_private_dataset():
    tables, budget, _m, _d = _private(_tables())
    people, orders = tables
    flat = repr((people.profile, orders.profile))
    assert str(_RECORDED) not in flat and "secret" not in flat and "SECRET" not in flat
    salary = people.profile.columns["salary"]
    assert 1500 < people.profile.row_count < 2500
    assert salary.min_value == "1" and salary.null_count < 100
    # The one huge salary moves no bound: the 99th percentile of the public grid is far below it.
    assert salary.sketch is not None and max(salary.sketch) < 1e6
    assert list(salary.sketch) == sorted(salary.sketch)
    fan = people.profile.fanouts[0]
    assert fan.child_table == "s.orders" and 0 <= fan.sketch[50] < 50


def test_the_same_seed_gives_the_same_statistics_and_another_seed_others():
    con = _con()
    a = _private(_tables(), seed=3, con=con)[0]
    b = _private(_tables(), seed=3, con=con)[0]
    c = _private(_tables(), seed=4, con=con)[0]
    assert a == b and a != c


@pytest.mark.parametrize(
    "fakes, message",
    [
        (
            {"tier": parse("categories()")},
            r"people.tier: declare its values, as categories\(\(a, b, c\)\)",
        ),
        ({"tier": parse("pattern()")}, r"people.tier: declare a fake other than pattern\(\)"),
        ({}, r"people.tier: declare a fake or a synthetic rule -- a method such as word\(\)"),
    ],
)
def test_what_would_release_values_from_an_unknown_domain_is_refused_by_name(fakes, message):
    with pytest.raises(DatasetRefused, match=message):
        _private(_tables(fakes))


def test_declared_values_and_booleans_take_noised_shares_of_public_values():
    tables, _b, _m, _d = _private(_tables({"tier": parse("categories((gold, silver))")}))
    tier = tables[0].profile.columns["tier"]
    assert {v for v, _ in tier.frequencies} == {"gold", "silver"}
    active = tables[0].profile.columns["active"]
    assert {v for v, _ in active.frequencies} <= {"true", "false"}


def test_the_public_grid_does_not_depend_on_the_data():
    assert grid("number") == grid("number") and len(grid("date")) == 201


def test_every_planned_statistic_is_charged_and_the_charges_sum_to_epsilon():
    _t, budget, _m, _d = _private(_tables(), epsilon=3.0)
    assert abs(budget.charged - 3.0) < 1e-9


def test_the_report_states_epsilon_and_its_split_with_no_caveat():
    _t, budget, _m, _d = _private(_tables())
    rows = report_entries(budget)
    [charged] = [r for r in rows if r["measure"] == "privacy_epsilon_charged"]
    assert abs(charged["synthetic_value"] - 50.0) < 1e-9
    [eps] = [r for r in rows if r["measure"] == "privacy_epsilon"]
    assert eps["synthetic_value"] == 50.0 and "not noised" not in eps["note"]
    families = [r for r in rows if r["measure"] == "privacy_epsilon_family"]
    assert abs(sum(r["synthetic_value"] for r in families) - 50.0) < 1e-9
    [none] = report_entries(None)
    assert none["measure"] == "privacy_guarantee" and none["note"].startswith("none")


def _large():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA s")
    con.execute(
        "CREATE TABLE s.big AS SELECT i AS id, 1000.0 + i AS amount FROM range(100000) AS t(i)"
    )
    table = DatasetTable(
        table_id=9,
        name="big",
        pgwire_name="s.big",
        columns=(("id", "integer", True), ("amount", "double", False)),
        scale=1.0,
        profile=ProfiledTable(
            "r9",
            7,
            7,
            {"id": _recorded("id", "numeric"), "amount": _recorded("amount", "numeric")},
            (),
        ),
    )
    return con, table


def test_at_epsilon_one_over_a_large_table_the_bounds_land_near_the_true_percentiles():
    con, table = _large()
    [big], budget, _m, _d = asyncio.run(
        private_tables(
            [table],
            [],
            epsilon=1.0,
            seed=5,
            governed=_governed(con),
            exposed=lambda t, c: c,
            qualified=lambda name: name,
        )
    )
    sketch = big.profile.columns["amount"].sketch
    assert sketch is not None
    p1, p99 = 1000.0 + 1000, 1000.0 + 99000  # the column's true 1st and 99th percentiles
    assert 0.75 * p1 <= sketch[0] <= 1.25 * p1, sketch[0]
    assert 0.75 * p99 <= sketch[100] <= 1.25 * p99, sketch[100]
    assert 0.9 * 51000 <= sketch[50] <= 1.1 * 51000, sketch[50]
    # A hundred thousand values: the noised distinct count decided no shares are measured, so no
    # ε is spent on them and the rest is spread over what generation uses.
    assert "shares" not in budget.families
    assert abs(budget.charged - 1.0) < 1e-9
    assert abs(sum(budget.families.values()) - 1.0) < 1e-9
    flagged = [r for r in report_entries(budget) if r["measure"] == "privacy_mostly_noise"]
    assert not [r for r in flagged if r["column_name"] == "amount"], flagged


def test_a_statistic_that_is_mostly_noise_is_flagged_in_the_report():
    _t, budget, _m, _d = _private(_tables(), epsilon=0.05)
    flagged = [r for r in report_entries(budget) if r["measure"] == "privacy_mostly_noise"]
    assert flagged
    assert "is comparable to its value" in flagged[0]["note"]
    assert "a declared distribution as the column's synthetic rule" in flagged[0]["note"]
