# Copyright (c) 2026 Kenneth Stott
# Canary: 5fd27061-cf52-4639-9c53-ebd64bf1109c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Synthetic generation computes every column's synthetic rule, else its fake, over the generated
row as a faked read computes it (REQ-1494, REQ-1939): methods, distributions, relative fakes with
a stated or measured distance, profile(), pattern(), hash() of the generated value and stable fakes;
and the table's accepted constraints bind it -- never null, unique, and an ordering generated as
before or less_than. Run on an in-process DuckDB with the engine's fake functions."""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import replace

import duckdb
import pytest

from provisa.fakes.duckdb_functions import register
from provisa.fakes.kinds import FakeKind, parse
from provisa.fakes.measured import DISTANCE_POINTS
from provisa.profiler.constraints import AcceptedConstraint, Constraint
from provisa.synthetic.generate import children_count_sql, generation_sql
from provisa.synthetic.plan import (
    DatasetRefused,
    DatasetTable,
    ProfiledColumn,
    ProfiledTable,
    distances_to_measure,
    plan_tables,
)

_COUNTER = duckdb.connect()


def _col(physical: str, family: str = "numeric", **kw) -> ProfiledColumn:
    base = dict(
        physical=physical,
        family=family,
        null_count=0,
        distinct_count=900,
        distinct_ratio=0.9,
        integer_only=False,
        min_value="1",
        sketch=None,
        top=(),
        frequencies=(),
        shapes=(),
    )
    base.update(kw)
    return ProfiledColumn(**base)


_DAY = 86400.0
_JAN = 1704067200.0  # 2024-01-01


def _orders(fakes: dict[str, FakeKind], **extra) -> DatasetTable:
    return DatasetTable(
        table_id=1,
        name="orders",
        pgwire_name="sales.orders",
        columns=(
            ("id", "integer", True),
            ("email", "varchar", False),
            ("region", "varchar", False),
            ("amount", "double", False),
            ("placed", "timestamp", False),
            ("shipped", "timestamp", False),
            ("code", "varchar", False),
        ),
        scale=1.0,
        fakes=fakes,
        profile=ProfiledTable(
            "r1",
            1000,
            1000,
            {
                "id": _col("id", min_value="1"),
                "email": _col("email", "text", null_count=100, shapes=(("aaaa@aaaa.aaa", 1),)),
                "region": _col(
                    "region",
                    "text",
                    distinct_count=3,
                    distinct_ratio=0.003,
                    frequencies=(("east", 500), ("west", 300), ("north", 200)),
                    shapes=(("aaaa", 1),),
                ),
                "amount": _col(
                    "amount", null_count=200, sketch=tuple(float(i * 2) for i in range(101))
                ),
                "placed": _col(
                    "placed",
                    "temporal",
                    sketch=tuple(_JAN + i * _DAY for i in range(101)),
                ),
                "shipped": _col(
                    "shipped",
                    "temporal",
                    sketch=tuple(_JAN + i * _DAY for i in range(101)),
                ),
                "code": _col("code", "text", shapes=(("AA-99", 3), ("a9", 1))),
            },
            (),
        ),
        **extra,
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


def _rows(con, table: DatasetTable, distance=lambda t, c: None) -> list[dict]:
    [planned] = plan_tables(
        [table],
        [],
        seed=7,
        names={1: "orders"},
        count_rows=_count,
        measure=_unmeasured,
        distance=distance,
    )
    res = con.execute(generation_sql(planned.plan, "duckdb"))
    names = [d[0] for d in res.description]
    return [dict(zip(names, r)) for r in res.fetchall()]


def test_a_method_fake_generates_every_value_and_keeps_the_null_share(con):
    rows = _rows(con, _orders({"email": parse("email()")}))
    emails = [r["email"] for r in rows]
    nulls = sum(e is None for e in emails)
    assert 50 <= nulls <= 150  # the profiled tenth
    assert all("@" in e for e in emails if e is not None)
    assert len({e for e in emails if e is not None}) == len(emails) - nulls  # tagged, distinct
    assert rows == _rows(con, _orders({"email": parse("email()")}))  # the same seed, the same rows


def test_distributions_profile_and_pattern_generate_in_their_ranges(con):
    rows = _rows(
        con,
        _orders(
            {
                "amount": parse("uniform(min=10, max=20)"),
                "placed": parse("profile()"),
                "code": parse("pattern()"),
            }
        ),
    )
    assert all(10 <= r["amount"] <= 20 for r in rows if r["amount"] is not None)
    assert all(dt.datetime(2024, 1, 1) <= r["placed"] <= dt.datetime(2024, 4, 10) for r in rows)
    shapes = re.compile(r"^([A-Z]{2}-[0-9]{2}|[a-z][0-9])$")
    assert all(shapes.match(r["code"]) for r in rows)
    assert {len(r["code"]) for r in rows} == {5, 2}


def test_a_relative_fake_follows_the_generated_column_by_a_stated_or_measured_distance(con):
    stated = _rows(con, _orders({"shipped": parse("after(placed, 1 to 3 days)")}))
    for r in stated:
        assert dt.timedelta(days=1) <= r["shipped"] - r["placed"] <= dt.timedelta(days=3)
    table = _orders({"shipped": parse("after(placed)")})
    assert [(t.name, c, o) for t, c, o in distances_to_measure([table], [])] == [
        ("orders", "shipped", "placed")
    ]
    quantiles = [2 * _DAY + q * _DAY for q in DISTANCE_POINTS]
    for r in _rows(con, table, distance=lambda t, c: quantiles):
        assert dt.timedelta(days=2) <= r["shipped"] - r["placed"] <= dt.timedelta(days=3)


def test_hash_computes_from_the_generated_value(con):
    rows = _rows(con, _orders({"region": parse("hash()")}))
    assert 2 <= len({r["region"] for r in rows}) <= 3  # one hash per generated region


def test_a_stable_fake_generates_by_the_portable_definition(con):
    table = _orders({"email": parse("name()")}, stable={"email": 1})
    rows = _rows(con, table)
    names = [r["email"] for r in rows if r["email"] is not None]
    assert all(len(n.split(" ")) == 2 for n in names)
    assert rows == _rows(con, table)


def _accepted(kind: str, column: str, other: str | None = None) -> AcceptedConstraint:
    return AcceptedConstraint(
        f"{kind}:{column}", Constraint(kind, column, other, {}), "", 1.0, False, column, other
    )


def test_accepted_constraints_bind_generation(con):
    table = _orders(
        {},
        constraints=(
            _accepted("not_null", "amount"),
            _accepted("unique", "code"),
            _accepted("ordering", "placed", "shipped"),
        ),
    )
    quantiles = [-(3 * _DAY) + q * 2 * _DAY for q in DISTANCE_POINTS]  # placed 1 to 3 days before
    rows = _rows(con, table, distance=lambda t, c: quantiles)
    assert all(r["amount"] is not None for r in rows)
    assert len({r["code"] for r in rows}) == len(rows)
    for r in rows:
        assert dt.timedelta(days=1) <= r["shipped"] - r["placed"] <= dt.timedelta(days=3)


def test_a_constraint_does_not_override_a_declared_fake(con):
    table = _orders(
        {"placed": parse("uniform(min='2025-01-01', max='2025-01-02')")},
        constraints=(_accepted("ordering", "placed", "shipped"),),
    )
    assert distances_to_measure([table], []) == []
    for r in _rows(con, table):
        assert dt.datetime(2025, 1, 1) <= r["placed"] <= dt.datetime(2025, 1, 2)


def test_a_value_kind_over_an_unprofiled_column_is_refused_by_name(con):
    table = _orders({"code": parse("prefix(2)")})
    table = replace(
        table,
        profile=replace(
            table.profile, columns={k: v for k, v in table.profile.columns.items() if k != "code"}
        ),
    )
    with pytest.raises(DatasetRefused, match=r"orders.code: prefix\(\) computes from the column"):
        _rows(con, table)


def test_the_trino_statement_with_fakes_parses():
    import sqlglot

    table = _orders(
        {
            "email": parse("email()"),
            "region": parse("hash()"),
            "shipped": parse("after(placed, 1 to 3 days)"),
            "code": parse("pattern()"),
        }
    )
    [planned] = plan_tables(
        [table], [], seed=7, names={1: "orders"}, count_rows=_count, measure=_unmeasured
    )
    sql = generation_sql(planned.plan, "trino")
    sqlglot.parse_one(sql, read="trino")
    low = sql.lower()
    assert "provisa_fake_method" in low and "provisa_seed" in low and "with_timezone" in low
