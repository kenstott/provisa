# Copyright (c) 2026 Kenneth Stott
# Canary: 61370ebd-8a0e-4973-9096-c6ead634807c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Faked reads (REQ-1494): a table with faked columns is read through one faked projection, so
every use of a faked column -- select list, filter, join, grouping, ordering, window, subquery,
relative fake -- reads the same fake; the row filter reads the real values. Run on DuckDB with the
engine's own fake functions."""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest
import sqlglot

from provisa.compiler import stage2
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.fakes import digest as digest_mod
from provisa.fakes.duckdb_functions import register
from provisa.fakes.read_sql import FakeReadRefused
from provisa.security.masking import MaskingRule, MaskType

_KEY = b"k" * 32
_COLUMNS = [
    ("id", "integer"),
    ("email", "varchar"),
    ("tier", "varchar"),
    ("score", "double"),
    ("created", "timestamp"),
    ("shipped", "timestamp"),
    ("qty", "integer"),
    ("price", "double"),
    ("total", "double"),
    ("region", "varchar"),
]
_FAKES = {
    "email": "email()",
    "tier": "categories((gold, silver))",
    "score": "uniform(min=0, max=10)",
    "created": "uniform(min='2024-01-01', max='2024-12-31')",
    "shipped": "after(created, 1 to 5 days)",
    "total": "sql(qty * price)",
    "qty": "poisson(mean=3)",
}


@pytest.fixture
def con(monkeypatch):
    monkeypatch.setattr(digest_mod, "_key", _KEY)
    c = duckdb.connect()
    register(c)
    c.execute("CREATE SCHEMA sales")
    c.execute(
        "CREATE TABLE sales.customers (id INTEGER, email VARCHAR, tier VARCHAR, score DOUBLE, "
        "created TIMESTAMP, shipped TIMESTAMP, qty INTEGER, price DOUBLE, total DOUBLE, "
        "region VARCHAR)"
    )
    for i in range(1, 201):
        c.execute(
            "INSERT INTO sales.customers VALUES (?, ?, 'bronze', ?, ?, ?, ?, 2.5, ?, ?)",
            [
                i,
                f"user{i % 150}@real.example",
                i / 10,
                dt.datetime(2020, 1, 1),
                dt.datetime(2020, 1, 2),
                i % 7,
                (i % 7) * 2.5,
                "east" if i % 2 else "west",
            ],
        )
    return c


def _gov(fakes=_FAKES, rls: str | None = None, stable: set[str] | None = None) -> GovernanceContext:
    gov = GovernanceContext(role_id="analyst")
    gov.table_map = {"sales.customers": 1, "customers": 1}
    gov.all_columns = {1: list(_COLUMNS)}
    gov.visible_columns = {1: None}
    for name, decl in fakes.items():
        dtype = dict(_COLUMNS)[name]
        gov.masking_rules[(1, name)] = (
            MaskingRule(MaskType.fake, fake=decl, fake_stable=name in (stable or set())),
            dtype,
        )
    if rls:
        gov.rls_rules = {1: rls}
    stage2._bind_fakes(gov)
    return gov


def _run(con, sql: str, gov=None):
    governed = apply_governance(sql, gov or _gov())
    duck = sqlglot.transpile(governed, read="postgres", write="duckdb")[0]
    return con.execute(duck).fetchall()


def test_a_faked_column_shows_one_fake_per_real_value_and_never_the_real_one(con):
    rows = _run(con, "SELECT id, email FROM sales.customers ORDER BY id")
    assert len(rows) == 200
    by_id = dict(rows)
    assert not {e for e in by_id.values()} & {f"user{i}@real.example" for i in range(150)}
    # ids 1 and 151 share a real email: they share its fake; distinct emails stay distinct.
    assert by_id[1] == by_id[151]
    assert len(set(by_id.values())) == 150


def test_filters_grouping_and_joins_read_the_fake(con):
    (fake,) = _run(con, "SELECT email FROM sales.customers WHERE id = 7")[0]
    found = _run(con, f"SELECT id FROM sales.customers WHERE email = '{fake}' ORDER BY id")
    assert [r[0] for r in found] == [7, 157]  # 157 holds the same real email as 7
    assert _run(con, "SELECT COUNT(*) FROM sales.customers WHERE email LIKE '%real.example'") == [
        (0,)
    ]
    tiers = dict(_run(con, "SELECT tier, COUNT(*) FROM sales.customers GROUP BY tier"))
    assert set(tiers) <= {"gold", "silver"} and sum(tiers.values()) == 200
    joined = _run(
        con,
        "SELECT COUNT(*) FROM sales.customers a JOIN sales.customers b ON a.email = b.email",
    )
    assert joined == [(200 + 2 * 50,)]  # 150 distinct emails, 50 of them held by two rows


def test_subqueries_and_windows_read_the_same_fake(con):
    rows = _run(
        con,
        "SELECT email, ROW_NUMBER() OVER (PARTITION BY tier ORDER BY score) AS n "
        "FROM sales.customers WHERE email IN (SELECT email FROM sales.customers WHERE id < 10)",
    )
    direct = {r[0] for r in _run(con, "SELECT email FROM sales.customers WHERE id < 10")}
    assert {r[0] for r in rows} == direct


def test_declared_distributions_land_in_range_and_type(con):
    rows = _run(con, "SELECT score, created, qty FROM sales.customers")
    assert all(0 <= s < 10 for s, _, _ in rows)
    assert all(dt.datetime(2024, 1, 1) <= c <= dt.datetime(2024, 12, 31) for _, c, _ in rows)
    assert all(isinstance(q, int) and q >= 0 for _, _, q in rows)


def test_relative_fakes_follow_the_faked_columns_they_name(con):
    rows = _run(con, "SELECT created, shipped, qty, price, total FROM sales.customers")
    for created, shipped, qty, price, total in rows:
        assert dt.timedelta(days=1) <= shipped - created <= dt.timedelta(days=5)
        assert total == pytest.approx(qty * price)


def test_the_row_filter_reads_the_real_values(con):
    gov = _gov(rls="email LIKE 'user1%'")
    rows = _run(con, "SELECT id FROM sales.customers", gov)
    real = {i for i in range(1, 201) if f"user{i % 150}".startswith("user1")}
    assert {r[0] for r in rows} == real


def test_a_stable_or_measured_fake_is_refused_by_name_until_it_is_computed(con):
    with pytest.raises(FakeReadRefused, match="email: a stable fake"):
        _run(con, "SELECT email FROM sales.customers", _gov({"email": "email()"}, stable={"email"}))
    with pytest.raises(FakeReadRefused, match="tier: categories\\(\\) takes measured values"):
        _run(con, "SELECT tier FROM sales.customers", _gov({"tier": "categories()"}))


def test_unfaked_columns_and_unfaked_tables_are_read_as_they_are(con):
    rows = _run(con, "SELECT id, region FROM sales.customers WHERE region = 'east' ORDER BY id")
    assert [r[0] for r in rows] == [i for i in range(1, 201) if i % 2]


def test_an_engine_without_the_fake_functions_refuses_by_column_name(monkeypatch):
    monkeypatch.setattr(digest_mod, "_key", _KEY)
    from provisa.fakes.read_sql import require_fake_engine

    sql = apply_governance("SELECT id, email FROM sales.customers", _gov({"email": "email()"}))
    require_fake_engine(sql, "duckdb", computes_fakes=True)
    with pytest.raises(FakeReadRefused, match="email: faked columns are computed by the engine"):
        require_fake_engine(sql, "postgres", computes_fakes=False)
    require_fake_engine("SELECT 1", "postgres", computes_fakes=False)
