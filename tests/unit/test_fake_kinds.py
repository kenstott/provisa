# Copyright (c) 2026 Kenneth Stott
# Canary: 372bf4d0-31e5-4f69-8087-6c55ac4506ff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's kind of fake as written, and the checks it passes when saved (REQ-1494)."""

from __future__ import annotations

import math

import pytest

from provisa.fakes.checks import Child, DeclaredColumn, Join, check_joins, check_table
from provisa.fakes.kinds import (
    Bool,
    Bucket,
    Categories,
    Distance,
    Encrypt,
    FakeRefused,
    Hash,
    LogNormal,
    Method,
    Normal,
    Ordered,
    Pattern,
    Percentiles,
    Poisson,
    Prefix,
    Profile,
    Sequence,
    Sql,
    Triangular,
    Truncate,
    Uniform,
    parse,
)


@pytest.mark.parametrize(
    "text,kind",
    [
        ("categories()", Categories()),
        ("categories((shoes, bra, sweater))", Categories(("shoes", "bra", "sweater"))),
        ("categories(('a b', \"c\"), (.25, .75))", Categories(("a b", "c"), (0.25, 0.75))),
        ("bool()", Bool()),
        ("bool(.8)", Bool(0.8)),
        (
            "percentiles(min=0, p50=10, max=100)",
            Percentiles(((0.0, 0.0), (0.5, 10.0), (1.0, 100.0))),
        ),
        ("normal(mean=50, sd=10, min=0)", Normal(50.0, 10.0, 0.0, None)),
        ("lognormal(mu=1, sigma=0.5)", LogNormal(1.0, 0.5)),
        ("uniform(min=1, max=2)", Uniform(1.0, 2.0)),
        ("triangular(min=1, mode=2, max=5)", Triangular(1.0, 2.0, 5.0)),
        ("poisson(mean=3)", Poisson(3.0)),
        ("profile()", Profile()),
        ("profile(run='r-1')", Profile("r-1")),
        ("bucket(10)", Bucket(width=10.0)),
        ("bucket((0, 18, 65))", Bucket(edges=(0.0, 18.0, 65.0))),
        ("truncate(month)", Truncate("month")),
        ("prefix(3)", Prefix(3)),
        ("pattern()", Pattern()),
        ("hash()", Hash()),
        ("encrypt()", Encrypt()),
        ("after(created_at)", Ordered("after", "created_at")),
        (
            "after(created_at, 3 days)",
            Ordered("after", "created_at", Distance(3.0, None, "day")),
        ),
        (
            "before(due, 1 to 10 days)",
            Ordered("before", "due", Distance(1.0, 10.0, "day")),
        ),
        ("greater_than(cost, 5)", Ordered("greater_than", "cost", Distance(5.0, None, None))),
        ("less_than(cost, 1 to 2)", Ordered("less_than", "cost", Distance(1.0, 2.0, None))),
        ("sql(quantity * price)", Sql("quantity * price")),
        (
            "sql_group(SUM(lines.amount), fake=uniform(min=0, max=9))",
            Sql("SUM(lines.amount)", group=True, fake=Uniform(0.0, 9.0)),
        ),
        (
            "sequence((placed, shipped, delivered), order_id, at)",
            Sequence(
                ("placed", "shipped", "delivered"),
                "order_id",
                "at",
                Categories(("placed", "shipped", "delivered"), (1 / 3, 1 / 3, 1 / 3)),
            ),
        ),
        (
            "sequence((a, b), e, o, fake=categories((a, b), (.9, .1)))",
            Sequence(("a", "b"), "e", "o", Categories(("a", "b"), (0.9, 0.1))),
        ),
        ("email()", Method("email")),
        ("pyint(min_value=1, max_value=9)", Method("pyint", (("max_value", 9), ("min_value", 1)))),
    ],
)
def test_each_kind_is_read_as_written(text, kind):
    assert parse(text) == kind


def test_lognormal_from_median_and_p95():
    k = parse("lognormal(median=10, p95=40)")
    assert isinstance(k, LogNormal)
    assert k.mu == pytest.approx(math.log(10))
    assert math.exp(k.mu + 1.6448536269514722 * k.sigma) == pytest.approx(40)


@pytest.mark.parametrize(
    "text,message",
    [
        ("email", "write it as kind\\(arguments\\)"),
        ("categories((a, b), (.5, .6))", "states shares summing to 1.1"),
        ("categories((a, b, c), (.5, .5))", "states 2 shares for 3 values"),
        ("categories((a, b), (1.5, -.5))", "states shares outside \\[0, 1\\]"),
        ("categories((a, a))", "names a value twice"),
        ("bool(1.2)", "outside \\[0, 1\\]"),
        ("percentiles(min=0, max=1)", "needs three or more"),
        ("percentiles(min=0, p50=5, max=1)", "max 1 is below p50 5"),
        ("normal(mean=1, sd=0)", "standard deviation 0 is not above zero"),
        ("normal(mean=1, sd=1, min=5, max=2)", "min 5 is not below max 2"),
        ("lognormal(median=10, p95=5)", "0 < median < p95"),
        ("lognormal(mu=1)", "needs median and p95, or mu and sigma"),
        ("triangular(min=1, mode=9, max=5)", "mode 9 is outside"),
        ("poisson(mean=0)", "not above zero"),
        ("bucket((5, 1))", "not in ascending order"),
        ("bucket(0)", "width 0 is not above zero"),
        ("truncate(fortnight)", "needs a unit"),
        ("prefix(0)", "whole length of one or more"),
        ("after(created_at, 3)", "needs a unit such as days"),
        ("after(created_at, 3 parsecs)", "'parsecs' is not a unit of time"),
        ("greater_than(cost, 5 to 1)", "runs backwards"),
        ("hash(1)", "at most 0 positional"),
        ("normal(mean=1, sd=1, skew=2)", "takes no argument 'skew'"),
        ("email(1)", "named arguments only"),
        ("sql()", "needs an expression"),
        ("sql_group(SUM(lines.amount))", "needs fake=<row fake>"),
        ("sql_group(a, b, fake=hash())", "needs one expression"),
        ("sql_group(a, fake=hash(), fake=hash())", "names fake= twice"),
        ("sql_group(a, fake=sql_group(b, fake=hash()))", "itself a cross-row fake"),
        ("sequence((a, b), e)", "needs \\(states\\), the entity column and the order column"),
        ("sequence((a, a), e, o)", "names a state twice"),
    ],
)
def test_a_declaration_that_cannot_describe_values_is_refused_by_name(text, message):
    with pytest.raises(FakeRefused, match=message):
        parse(text)


def _table(*cols: DeclaredColumn, children=None):
    return check_table("orders", list(cols), children or {})


def test_a_fake_must_fit_its_column_type():
    with pytest.raises(FakeRefused, match="orders.flag: bool\\(\\) fakes a boolean column"):
        _table(DeclaredColumn("flag", "varchar", "bool()"))
    with pytest.raises(FakeRefused, match="orders.n: the value 'x' is not a integer"):
        _table(DeclaredColumn("n", "integer", "categories((1, x))"))
    with pytest.raises(FakeRefused, match="pattern\\(\\) fakes a text column"):
        _table(DeclaredColumn("n", "integer", "pattern()"))
    with pytest.raises(FakeRefused, match="a date has no hour"):
        _table(DeclaredColumn("d", "date", "truncate(hour)"))
    kinds = _table(DeclaredColumn("n", "bigint", "poisson(mean=2)"))
    assert kinds == {"n": Poisson(2.0)}


def test_a_fake_method_is_checked_against_the_column():
    assert _table(DeclaredColumn("e", "varchar", "email()")) == {"e": Method("email")}
    with pytest.raises(FakeRefused, match="no fake kind or method 'emial'"):
        _table(DeclaredColumn("e", "varchar", "emial()"))
    with pytest.raises(FakeRefused, match="email\\(\\) makes str values, which a integer column"):
        _table(DeclaredColumn("e", "integer", "email()"))
    with pytest.raises(FakeRefused, match="takes no argument 'bogus'"):
        _table(DeclaredColumn("e", "varchar", "email(bogus=1)"))


def test_a_relative_fake_names_a_column_of_the_table_of_a_type_it_can_follow():
    with pytest.raises(FakeRefused, match="names 'shipped', which the table does not hold"):
        _table(DeclaredColumn("due", "date", "after(shipped, 2 days)"))
    with pytest.raises(FakeRefused, match="names 'cost', a text column"):
        _table(
            DeclaredColumn("cost", "varchar"),
            DeclaredColumn("price", "double", "greater_than(cost)"),
        )


def test_references_that_form_a_cycle_are_refused_naming_the_columns():
    with pytest.raises(
        FakeRefused, match=r"a, c, b name one another in a cycle \(a -> c -> b -> a\)"
    ):
        _table(
            DeclaredColumn("a", "date", "after(c)"),
            DeclaredColumn("b", "date", "after(a)"),
            DeclaredColumn("c", "date", "sql(b + interval '1 day')"),
        )
    with pytest.raises(FakeRefused, match="x name one another in a cycle"):
        _table(DeclaredColumn("x", "integer", "sql(x + 1)"))


def test_the_sql_fake_is_held_to_its_subset():
    ok = _table(
        DeclaredColumn("quantity", "integer"),
        DeclaredColumn("price", "double"),
        DeclaredColumn("total", "double", "sql(round(quantity * price, 2))"),
    )
    assert ok["total"] == Sql("round(quantity * price, 2)")
    for expr, message in [
        ("random()", "uses rand"),
        ("now()", "not in the subset"),
        ("(select 1)", "subquery|statement"),
        ("sum(price)", "only sql_group may use"),
        ("my_udf(price)", "calls my_udf\\(\\)"),
        ("other.price", "a child's column is read only within"),
        ("missing + 1", "names 'missing', which the table does not hold"),
    ]:
        with pytest.raises(FakeRefused, match=message):
            _table(DeclaredColumn("price", "double"), DeclaredColumn("t", "double", f"sql({expr})"))


def test_sql_group_reads_windows_and_a_parents_children():
    children = {"lines": Child("order_lines", {"amount": "double"})}
    kinds = _table(
        DeclaredColumn("id", "integer"),
        DeclaredColumn("customer_id", "integer"),
        DeclaredColumn("amount", "double"),
        DeclaredColumn(
            "total", "double", "sql_group(SUM(lines.amount), fake=uniform(min=0, max=9))"
        ),
        DeclaredColumn(
            "balance",
            "double",
            "sql_group(sum(self) over (partition by customer_id order by id), fake=normal(mean=0, sd=5))",
        ),
        children=children,
    )
    assert set(kinds) == {"total", "balance"}
    with pytest.raises(FakeRefused, match="reads 'items', which is no relationship"):
        _table(
            DeclaredColumn("t", "double", "sql_group(SUM(items.amount), fake=hash())"),
            children=children,
        )
    with pytest.raises(FakeRefused, match="names lines.qty, which order_lines does not hold"):
        _table(
            DeclaredColumn("t", "double", "sql_group(SUM(lines.qty), fake=hash())"),
            children=children,
        )


def test_stable_is_limited_to_what_every_engine_computes_alike():
    assert _table(DeclaredColumn("e", "varchar", "email()", stable=True))
    with pytest.raises(FakeRefused, match="ipv4\\(\\) cannot be stable"):
        _table(DeclaredColumn("e", "varchar", "ipv4()", stable=True))
    with pytest.raises(FakeRefused, match="pins the run it reads"):
        _table(DeclaredColumn("n", "double", "profile()", stable=True))
    with pytest.raises(FakeRefused, match="declared stable but declares no fake"):
        _table(DeclaredColumn("n", "double", None, stable=True))


def test_joined_columns_declare_one_fake():
    join = Join("cust", ("orders", "customer_id"), ("customers", "id"))
    check_joins(
        {("orders", "customer_id"): (Hash(), True), ("customers", "id"): (Hash(), True)}, [join]
    )
    check_joins({("orders", "customer_id"): (Hash(), True)}, [join])
    with pytest.raises(
        FakeRefused, match="joins orders.customer_id to customers.id, whose fakes differ"
    ):
        check_joins(
            {("orders", "customer_id"): (Hash(), True), ("customers", "id"): (Encrypt(), True)},
            [join],
        )
    with pytest.raises(FakeRefused, match="differ in stable"):
        check_joins(
            {("orders", "customer_id"): (Hash(), True), ("customers", "id"): (Hash(), False)},
            [join],
        )


def test_a_sequence_names_columns_of_its_table_and_states_its_type_holds():
    kinds = _table(
        DeclaredColumn("order_id", "integer"),
        DeclaredColumn("at", "timestamp"),
        DeclaredColumn("status", "varchar", "sequence((placed, shipped), order_id, at)"),
    )
    assert isinstance(kinds["status"], Sequence)
    with pytest.raises(
        FakeRefused, match="sequence\\(\\) names 'at', which the table does not hold"
    ):
        _table(
            DeclaredColumn("order_id", "integer"),
            DeclaredColumn("status", "varchar", "sequence((placed, shipped), order_id, at)"),
        )


def test_sql_group_names_its_drawn_value_self_and_its_own_name_is_a_cycle():
    _table(DeclaredColumn("x", "double", "sql_group(self * 2, fake=uniform(min=0, max=1))"))
    with pytest.raises(FakeRefused, match="x name one another in a cycle"):
        _table(DeclaredColumn("x", "double", "sql_group(x * 2, fake=uniform(min=0, max=1))"))


_DAY = 86400.0
_JAN1 = 1704067200.0  # 2024-01-01T00:00:00Z


def test_declared_distributions_over_dates_take_dates_and_intervals():
    """REQ-1494, DECLARED DISTRIBUTIONS: a date or time column's distribution is declared with
    dates or times for its points and intervals for its spreads."""
    assert parse("uniform(min='2024-01-01', max='2024-01-31')") == Uniform(
        _JAN1, _JAN1 + 30 * _DAY, temporal=True
    )
    assert parse("normal(mean='2024-01-01', sd=10 days)") == Normal(_JAN1, 10 * _DAY, temporal=True)
    assert parse("triangular(min='2024-01-01', mode='2024-01-02', max='2024-01-11')") == (
        Triangular(_JAN1, _JAN1 + _DAY, _JAN1 + 10 * _DAY, temporal=True)
    )
    pct = parse("percentiles(min='2024-01-01', p50='2024-01-02T12:00', max='2024-01-05')")
    assert isinstance(pct, Percentiles) and pct.temporal
    assert pct.points[1] == (0.5, _JAN1 + 1.5 * _DAY)
    ln = parse("lognormal(min='2024-01-01', median='2024-01-03', p95='2024-01-21')")
    assert isinstance(ln, LogNormal) and ln.temporal
    assert ln.mu == pytest.approx(math.log(2 * _DAY))


@pytest.mark.parametrize(
    "text,message",
    [
        (
            "uniform(min='2024-02-01', max='2024-01-01')",
            "min 2024-02-01T00:00:00\\+00:00 is not below",
        ),
        ("uniform(min='2024-01-01', max=5)", "mixes dates and numbers"),
        ("normal(mean='2024-01-01', sd=10)", "sd 10.0 needs a unit"),
        ("normal(mean='2024-01-01', sd=10 fortnights)", "'fortnights' is not a unit of time"),
        ("uniform(min='soon', max='later')", "min 'soon' is not a date or time"),
        ("lognormal(median='2024-01-03', p95='2024-01-21')", "needs min, the moment"),
    ],
)
def test_temporal_arguments_that_cannot_describe_a_distribution_are_refused(text, message):
    with pytest.raises(FakeRefused, match=message):
        parse(text)


def test_a_distribution_fits_its_column_by_the_kind_of_its_points():
    assert _table(DeclaredColumn("d", "date", "uniform(min='2024-01-01', max='2024-02-01')"))
    assert _table(DeclaredColumn("t", "timestamp", "normal(mean='2024-01-01', sd=2 hours)"))
    with pytest.raises(FakeRefused, match="uniform\\(\\) fakes a date or timestamp column"):
        _table(DeclaredColumn("n", "integer", "uniform(min='2024-01-01', max='2024-02-01')"))
    with pytest.raises(FakeRefused, match="uniform\\(\\) fakes a integer or numeric column"):
        _table(DeclaredColumn("d", "date", "uniform(min=1, max=2)"))
