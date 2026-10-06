# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3f9e02-5c18-4d6a-a1e4-c0d8b2f5e971
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Synthetic generation (REQ-1939): the plan a profile run gives each table, and the one statement
that generates it, run on an in-process DuckDB and checked against what was profiled."""

from __future__ import annotations

import sys
from collections import Counter
from dataclasses import replace

import duckdb
import pytest
import sqlglot

from provisa.fakes.kinds import Bool, Categories, FakeKind, FakeRefused, parse
from provisa.profiler import measures
from provisa.synthetic.generate import children_count_sql, generation_sql
from provisa.synthetic.plan import (
    DatasetRefused,
    DatasetTable,
    Edge,
    ProfiledColumn,
    ProfiledFanout,
    ProfiledTable,
    check_closure,
    edges_of,
    plan_tables,
    to_measure,
)
from provisa.synthetic.report import ks_between

# The fakes declared on the tables a test builds (REQ-1494); set by _declare.
_DECLARED: dict[str, FakeKind] = {}

_UNIFORM_100 = tuple(float(i) for i in range(101))
# Children per parent: half the parents have none, the top 1% have 50 (the hot keys).
_FANOUT = tuple(0.0 if i < 50 else (5.0 if i < 99 else 50.0) for i in range(101))


def _col(physical: str, family: str = "numeric", **kw) -> ProfiledColumn:
    base = dict(
        physical=physical,
        family=family,
        null_count=0,
        distinct_count=100,
        distinct_ratio=1.0,
        integer_only=True,
        min_value="1",
        sketch=None,
        top=(),
        frequencies=(),
        shapes=(),
    )
    base.update(kw)
    return ProfiledColumn(**base)


def _customers(scale: float = 2.0) -> DatasetTable:
    return DatasetTable(
        table_id=1,
        name="customers",
        pgwire_name="sales.customers",
        columns=(
            ("id", "integer", True),
            ("region", "varchar", False),
            ("email", "varchar", False),
        ),
        scale=scale,
        fakes=dict(_DECLARED),
        profile=ProfiledTable(
            "r1",
            1000,
            1000,
            {
                "id": _col("id", sketch=_UNIFORM_100),
                "region": _col(
                    "region",
                    "text",
                    distinct_count=3,
                    distinct_ratio=0.003,
                    frequencies=(("east", 500), ("west", 300), ("north", 200)),
                    shapes=(("aaaa", 700), ("aaaaa", 300)),
                ),
                "email": _col(
                    "email",
                    "text",
                    null_count=100,
                    distinct_count=900,
                    distinct_ratio=0.9,
                    shapes=(("aaaa@aaaaaaa.aaa", 800), ("aaa@aaaaaaa.aaa", 100)),
                ),
            },
            (ProfiledFanout("sales.purchases", 1000, _FANOUT),),
        ),
    )


def _purchases() -> DatasetTable:
    return DatasetTable(
        table_id=2,
        name="purchases",
        pgwire_name="sales.purchases",
        columns=(
            ("id", "integer", True),
            ("customer_id", "integer", False),
            ("amount", "double", False),
        ),
        scale=2.0,
        fakes=dict(_DECLARED),
        profile=ProfiledTable(
            "r2",
            3000,
            3000,
            {
                "id": _col("id", min_value="100"),
                "customer_id": _col("customer_id"),
                "amount": _col(
                    "amount",
                    null_count=300,
                    integer_only=False,
                    sketch=tuple(v * 2 for v in _UNIFORM_100),
                ),
            },
            (),
        ),
    )


_EDGES = [Edge(1, "id", 2, "customer_id")]
_NAMES = {1: "customers", 2: "purchases"}
_COUNTER = duckdb.connect()


def _count(plan) -> int:
    return _COUNTER.execute(children_count_sql(plan, "duckdb")).fetchone()[0]


def _unmeasured(t, column):
    raise AssertionError(f"{t.name}.{column} was read from its table")


def _run(plan, con):
    res = con.execute(generation_sql(plan, "duckdb"))
    names = [d[0] for d in res.description]
    return [dict(zip(names, r)) for r in res.fetchall()]


@pytest.fixture
def con():
    return duckdb.connect()


def test_rows_follow_the_scale_and_children_follow_the_fanout(con):
    planned = plan_tables(
        [_purchases(), _customers()],
        _EDGES,
        seed=7,
        names=_NAMES,
        count_rows=_count,
        measure=_unmeasured,
    )
    assert [p.table.name for p in planned] == ["customers", "purchases"]  # parents first
    customers, purchases = (_run(p.plan, con) for p in planned)
    assert len(customers) == 2000
    assert len({c["id"] for c in customers}) == 2000 and min(c["id"] for c in customers) == 1
    per_parent: dict[int, int] = {}
    for p in purchases:
        per_parent[p["customer_id"]] = per_parent.get(p["customer_id"], 0) + 1
    assert set(per_parent) <= {c["id"] for c in customers}  # every child has a generated parent
    assert max(per_parent.values()) == 50  # the hot key survives
    childless = 2000 - len(per_parent)
    assert 0.45 <= childless / 2000 <= 0.55
    assert len({p["id"] for p in purchases}) == len(purchases)
    assert min(p["id"] for p in purchases) == 100


def test_values_follow_their_profile(con):
    [cplan] = [
        p
        for p in plan_tables(
            [_customers()], [], seed=7, names=_NAMES, count_rows=_count, measure=_unmeasured
        )
    ]
    rows = _run(cplan.plan, con)
    regions = Counter(r["region"] for r in rows)
    # No fake kind: three generated values with the profiled shares, none of them a real one.
    assert len(regions) == 3 and not {"east", "west", "north"} & set(regions)
    assert sorted(n / len(rows) for n in regions.values()) == pytest.approx(
        [0.2, 0.3, 0.5], abs=0.04
    )
    emails = [r["email"] for r in rows]
    assert sum(e is None for e in emails) / len(rows) == pytest.approx(0.1, abs=0.03)
    assert all("@" in e and e.count(".") == 1 for e in emails if e is not None)
    assert cplan.undeclared == ["id", "region", "email"]


def test_a_numeric_column_is_drawn_through_its_sketch(con):
    planned = plan_tables(
        [_customers(), _purchases()],
        _EDGES,
        seed=3,
        names=_NAMES,
        count_rows=_count,
        measure=_unmeasured,
    )
    rows = _run(planned[1].plan, con)
    amounts = sorted(r["amount"] for r in rows if r["amount"] is not None)
    sketch = [amounts[round(q * (len(amounts) - 1))] for q in (i / 100 for i in range(101))]
    assert ks_between(sketch, tuple(v * 2 for v in _UNIFORM_100), measures) < 0.05
    assert sum(r["amount"] is None for r in rows) / len(rows) == pytest.approx(0.1, abs=0.03)


def test_the_same_seed_gives_the_same_rows_and_another_seed_others(con):
    a = _run(
        plan_tables(
            [_customers()], [], seed=1, names=_NAMES, count_rows=_count, measure=_unmeasured
        )[0].plan,
        con,
    )
    b = _run(
        plan_tables(
            [_customers()], [], seed=1, names=_NAMES, count_rows=_count, measure=_unmeasured
        )[0].plan,
        con,
    )
    c = _run(
        plan_tables(
            [_customers()], [], seed=2, names=_NAMES, count_rows=_count, measure=_unmeasured
        )[0].plan,
        con,
    )
    assert a == b and a != c


def test_a_fraction_keeps_the_vocabulary_of_a_fixed_column(con):
    rows = _run(
        plan_tables(
            [_customers(scale=0.5)],
            [],
            seed=1,
            names=_NAMES,
            count_rows=_count,
            measure=_unmeasured,
        )[0].plan,
        con,
    )
    assert len(rows) == 500
    assert len({r["region"] for r in rows}) == 3


def test_closure_is_refused_naming_the_missing_parent():
    with pytest.raises(DatasetRefused, match="'purchases' refers to 'customers'"):
        check_closure({2: "purchases"}, {1: "customers", 2: "purchases"}, _EDGES)
    with pytest.raises(DatasetRefused, match="refers to 'customers'"):
        plan_tables(
            [_purchases()], _EDGES, seed=1, names=_NAMES, count_rows=_count, measure=_unmeasured
        )


def test_edges_are_read_from_either_direction_of_a_relationship():
    rels = [
        {
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "id",
            "target_column": "customer_id",
            "cardinality": "one-to-many",
            "via_table_id": None,
        },
        {
            "source_table_id": 2,
            "target_table_id": 1,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
            "via_table_id": None,
        },
    ]
    assert edges_of(rels) == [Edge(1, "id", 2, "customer_id")] * 2


def test_the_trino_statement_parses():
    for p in plan_tables(
        [_customers(), _purchases()],
        _EDGES,
        seed=7,
        names=_NAMES,
        count_rows=_count,
        measure=_unmeasured,
    ):
        assert sqlglot.parse_one(generation_sql(p.plan, "trino"), read="trino") is not None


def test_an_unsupported_engine_is_refused():
    with pytest.raises(ValueError, match="no synthetic generation for engine dialect 'clickhouse'"):
        generation_sql(
            plan_tables(
                [_customers()], [], seed=1, names=_NAMES, count_rows=_count, measure=_unmeasured
            )[0].plan,
            "clickhouse",
        )


def _with(t: DatasetTable, column: str, **changes) -> DatasetTable:
    cols = dict(t.profile.columns)
    cols[column] = replace(cols[column], **changes)
    return replace(t, profile=replace(t.profile, columns=cols))


def test_a_hot_parent_keeps_its_scaled_child_count(con):
    """The child's most frequent keys beyond the fan-out sketch keep their counts, scaled with
    the parents, on the first generated parents."""
    purchases = _with(_purchases(), "customer_id", top=(("17", 300), ("4", 3)))
    planned = plan_tables(
        [_customers(), purchases],
        _EDGES,
        seed=7,
        names=_NAMES,
        count_rows=_count,
        measure=_unmeasured,
    )
    assert planned[1].plan.fanout[2] == (600,)  # 300 children x scale 2; 3 is within the sketch
    rows = _run(planned[1].plan, con)
    assert sum(r["customer_id"] == 1 for r in rows) == 600  # the first generated customer
    assert len(rows) == _count(planned[1].plan)


def test_a_sampled_profile_scales_to_the_whole_table(con):
    sampled = replace(_customers(), profile=replace(_customers().profile, row_count=4000))
    [planned] = plan_tables(
        [sampled], [], seed=1, names=_NAMES, count_rows=_count, measure=_unmeasured
    )
    assert planned.plan.rows == 8000
    rows = _run(planned.plan, con)
    # Shares are over the rows the run read: 500 of 1000 is half, whatever the table holds.
    top = Counter(r["region"] for r in rows).most_common(1)[0][1]
    assert top / len(rows) == pytest.approx(0.5, abs=0.03)


def test_no_column_without_a_categories_fake_has_a_recorded_value(con):
    """A real value reaches a cell only through a declared categories fake (REQ-1939, CATEGORIES
    ARE DECLARED, NOT INFERRED): recorded values, a category's included, are never cell values."""
    customers = _with(_customers(), "email", top=(("ann@example.com", 5), ("bob@example.com", 4)))
    purchases = _with(_purchases(), "amount", top=(("7.0", 40),))
    planned = plan_tables(
        [customers, purchases], _EDGES, seed=1, names=_NAMES, count_rows=_count, measure=_unmeasured
    )
    crows, prows = (_run(p.plan, con) for p in planned)
    assert not {"ann@example.com", "bob@example.com"} & {r["email"] for r in crows}
    assert 7.0 not in {r["amount"] for r in prows}
    assert not {"east", "west", "north"} & {r["region"] for r in crows}


def _declare(monkeypatch, fakes: dict[str, FakeKind]) -> None:
    """Declare ``fakes`` on the tables the test builds next (REQ-1494)."""
    monkeypatch.setattr(sys.modules[__name__], "_DECLARED", fakes)


def _regions(customers: DatasetTable, con) -> Counter:
    [planned] = plan_tables(
        [customers], [], seed=1, names=_NAMES, count_rows=_count, measure=_unmeasured
    )
    return Counter(r["region"] for r in _run(planned.plan, con))


def _share(counts: Counter, value: str) -> float:
    return counts[value] / sum(counts.values())


def test_categories_from_the_profile_takes_its_full_frequency_table(con, monkeypatch):
    """categories(): the column's own recorded values at their recorded shares -- the declared
    fake is what lets a real value into a cell, and it lifts the pii bar."""
    _declare(monkeypatch, {"region": Categories()})
    regions = _regions(replace(_customers(), pii=frozenset({"region"})), con)
    assert set(regions) == {"east", "west", "north"}
    assert _share(regions, "east") == pytest.approx(0.5, abs=0.04)


_EMAILS = [("a@x.com", 60), ("b@x.com", 20), (None, 20)]


def _measured(counts):
    calls = []

    def measure(t, column):
        calls.append(f"{t.name}.{column}")
        return counts

    return measure, calls


def _emails(con, monkeypatch, fake, counts):
    _declare(monkeypatch, {"email": fake})
    measure, calls = _measured(counts)
    assert [f"{t.name}.{c}" for t, c in to_measure([_customers()], [])] == ["customers.email"]
    [planned] = plan_tables(
        [_customers()], [], seed=1, names=_NAMES, count_rows=_count, measure=measure
    )
    assert calls == ["customers.email"]
    return Counter(r["email"] for r in _run(planned.plan, con))


def test_categories_without_a_frequency_table_reads_them_from_the_table(con, monkeypatch):
    """REQ-1494, MEASURED FROM THE PROFILE, ELSE FROM THE TABLE: email has no full frequency
    table, so categories() takes its values and shares, nulls included, from one GROUP BY."""
    emails = _emails(con, monkeypatch, Categories(), _EMAILS)
    assert set(emails) == {"a@x.com", "b@x.com", None}
    assert _share(emails, "a@x.com") == pytest.approx(0.6, abs=0.04)
    assert _share(emails, None) == pytest.approx(0.2, abs=0.04)


def test_named_categories_take_their_shares_from_the_table(con, monkeypatch):
    """a@x.com measured at 3/4 of the values; z@x.com, unmeasured, takes the remaining 1/4."""
    emails = _emails(con, monkeypatch, Categories(("a@x.com", "z@x.com")), _EMAILS)
    assert set(emails) == {"a@x.com", "z@x.com", None}
    assert _share(emails, "a@x.com") == pytest.approx(0.6, abs=0.04)
    assert _share(emails, "z@x.com") == pytest.approx(0.2, abs=0.04)


def test_categories_over_an_empty_table_is_refused(monkeypatch):
    with pytest.raises(DatasetRefused, match=r"customers.email declares categories\(\) but its"):
        _emails(None, monkeypatch, Categories(), [])


def test_the_categories_fake_over_a_full_frequency_table_reads_no_table(monkeypatch):
    _declare(monkeypatch, {"region": Categories(), "email": Categories(("a", "b"), (0.5, 0.5))})
    assert to_measure([_customers()], []) == []


def _active(con, monkeypatch, fake, profiled=None, counts=None) -> float:
    """The share of true in a generated boolean column ``active``."""
    _declare(monkeypatch, {"active": fake} if fake is not None else {})
    customers = replace(
        _customers(),
        columns=_customers().columns + (("active", "boolean", False),),
        profile=replace(
            _customers().profile,
            columns={**_customers().profile.columns}
            | ({"active": profiled} if profiled is not None else {}),
        ),
    )
    measure, calls = _measured(counts) if counts is not None else (_unmeasured, [])
    [planned] = plan_tables(
        [customers], [], seed=1, names=_NAMES, count_rows=_count, measure=measure
    )
    assert calls == (["customers.active"] if counts is not None else [])
    values = Counter(r["active"] for r in _run(planned.plan, con))
    assert set(values) <= {True, False}
    return values[True] / sum(values.values())


def test_bool_with_a_stated_share(con, monkeypatch):
    """REQ-1494, THE BOOL FAKE: bool(.8) is true in 80% of rows."""
    assert _active(con, monkeypatch, Bool(0.8)) == pytest.approx(0.8, abs=0.04)


def test_an_undeclared_boolean_is_bool_at_its_profiled_share(con, monkeypatch):
    """REQ-1939, BOOLEANS: an undeclared boolean is bool(), its share from the profile."""
    profiled = _col("active", "boolean", frequencies=(("true", 300), ("false", 700)))
    assert _active(con, monkeypatch, None, profiled) == pytest.approx(0.3, abs=0.04)


def test_bool_without_a_profile_reads_its_share_from_the_table(con, monkeypatch):
    share = _active(con, monkeypatch, Bool(), counts=[("true", 90), ("false", 10)])
    assert share == pytest.approx(0.9, abs=0.03)


def test_bool_over_an_empty_table_is_even(con, monkeypatch):
    assert _active(con, monkeypatch, None, counts=[]) == pytest.approx(0.5, abs=0.05)


def test_a_bool_share_outside_zero_and_one_is_refused(con, monkeypatch):
    with pytest.raises(DatasetRefused, match="customers.active: the bool fake's share 1.5"):
        _active(con, monkeypatch, Bool(1.5))


def test_named_categories_take_profiled_shares_and_share_the_remainder(con, monkeypatch):
    """categories((east, west, south)): east and west at their profiled shares (0.5, 0.3);
    south, unrecorded, takes what remains (0.2)."""
    _declare(monkeypatch, {"region": Categories(("east", "west", "south"))})
    regions = _regions(_customers(), con)
    assert set(regions) == {"east", "west", "south"}
    assert _share(regions, "east") == pytest.approx(0.5, abs=0.04)
    assert _share(regions, "south") == pytest.approx(0.2, abs=0.04)


def test_stated_shares_win_over_the_profile(con, monkeypatch):
    _declare(monkeypatch, {"region": Categories(("shoes", "bra", "sweater"), (0.1, 0.6, 0.3))})
    regions = _regions(_customers(), con)
    assert _share(regions, "bra") == pytest.approx(0.6, abs=0.04)
    assert _share(regions, "shoes") == pytest.approx(0.1, abs=0.03)


@pytest.mark.parametrize(
    "shares,message",
    [
        ((0.5, 0.5), "states 2 shares for 3 values"),
        ((0.5, 0.6, -0.1), "states shares outside \\[0, 1\\]"),
        ((0.2, 0.2, 0.2), "states shares summing to"),
    ],
)
def test_stated_shares_that_cannot_describe_the_values_are_refused(shares, message):
    """Refused when declared, so no dataset ever plans with them (REQ-1494, CATEGORY WEIGHTS)."""
    declared = f"categories((a, b, c), ({', '.join(str(x) for x in shares)}))"
    with pytest.raises(FakeRefused, match=f"categories\\(\\): {message}"):
        parse(declared)


def test_a_fake_synthesis_does_not_compute_yet_is_refused_by_name(monkeypatch):
    _declare(monkeypatch, {"email": parse("email()")})
    with pytest.raises(
        DatasetRefused, match=r"customers.email declares email\(\), which synthetic"
    ):
        plan_tables(
            [_customers()], [], seed=1, names=_NAMES, count_rows=_count, measure=_unmeasured
        )
