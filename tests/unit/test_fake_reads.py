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
import re

import duckdb
import pytest
import sqlglot

from provisa.compiler import stage2
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.fakes import digest as digest_mod
from provisa.fakes.duckdb_functions import register
from provisa.fakes.measured import Measured
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
    ("active", "boolean"),
    ("code", "varchar"),
    ("twin", "double"),
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
        "region VARCHAR, active BOOLEAN, code VARCHAR, twin DOUBLE)"
    )
    for i in range(1, 201):
        c.execute(
            "INSERT INTO sales.customers VALUES (?, ?, 'bronze', ?, ?, ?, ?, 2.5, ?, ?, TRUE, ?, ?)",
            [
                i,
                f"user{i % 150}@real.example",
                i / 10,
                dt.datetime(2020, 1, 1),
                dt.datetime(2020, 1, 2),
                i % 7,
                (i % 7) * 2.5,
                "east" if i % 2 else "west",
                f"C{i}",
                i / 10,  # the same real values as score
            ],
        )
    return c


def _gov(
    fakes=_FAKES,
    rls: str | None = None,
    stable: set[str] | None = None,
    measured: dict[str, Measured] | None = None,
    stable_version: int | None = None,
) -> GovernanceContext:
    gov = GovernanceContext(role_id="analyst")
    gov.table_map = {"sales.customers": 1, "customers": 1}
    gov.all_columns = {1: list(_COLUMNS)}
    gov.visible_columns = {1: None}
    for name, decl in fakes.items():
        dtype = dict(_COLUMNS)[name]
        gov.masking_rules[(1, name)] = (
            MaskingRule(
                MaskType.fake,
                fake=decl,
                fake_stable=name in (stable or set()),
                fake_measured=(measured or {}).get(name),
                fake_stable_version=stable_version if name in (stable or set()) else None,
            ),
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


def test_a_stable_or_unmeasured_fake_is_refused_by_name(con):
    with pytest.raises(FakeReadRefused, match="email: a stable fake"):
        _run(con, "SELECT email FROM sales.customers", _gov({"email": "email()"}, stable={"email"}))
    with pytest.raises(FakeReadRefused, match="tier: categories\\(\\) computes from measured"):
        _run(con, "SELECT tier FROM sales.customers", _gov({"tier": "categories()"}))
    refused = Measured(
        refused="tier: categories() takes its values from the table, which holds none"
    )
    with pytest.raises(FakeReadRefused, match="which holds none"):
        _run(
            con,
            "SELECT tier FROM sales.customers",
            _gov({"tier": "categories()"}, measured={"tier": refused}),
        )


def test_measured_shares_values_and_distributions_are_what_the_read_shows(con):
    gov = _gov(
        {
            "tier": "categories()",
            "score": "profile()",
            "region": "profile()",
            "created": "profile()",
        },
        measured={
            "tier": Measured(values=(("gold", 0.7), ("silver", 0.3))),
            "score": Measured(points=((0.0, 100.0), (0.5, 150.0), (1.0, 200.0))),
            "region": Measured(values=(("north", 0.5), ("south", 0.5))),
            "created": Measured(points=((0.0, 1704067200.0), (1.0, 1735603200.0))),
        },
    )
    rows = _run(con, "SELECT active, tier, score, region, created FROM sales.customers", gov)
    assert len({r[1] for r in rows} & {"gold", "silver"}) == 1  # every real tier is bronze
    assert all(100.0 <= r[2] <= 200.0 for r in rows)
    assert {r[3] for r in rows} <= {"north", "south"}
    assert all(dt.datetime(2024, 1, 1) <= r[4] <= dt.datetime(2024, 12, 31) for r in rows)


@pytest.mark.parametrize("share, shown", [(0.0, False), (1.0, True)])
def test_bool_shows_true_at_the_measured_share(con, share, shown):
    # Every row holds TRUE, so every row shows the one fake of TRUE: the share decides which.
    gov = _gov({"active": "bool()"}, measured={"active": Measured(true_share=share)})
    assert {r[0] for r in _run(con, "SELECT active FROM sales.customers", gov)} == {shown}


def test_a_pattern_fills_the_measured_shapes(con):
    gov = _gov(
        {"code": "pattern()"},
        measured={"code": Measured(shapes=(("AA-99", 0.5), ("a.9@x%!", 0.5)))},
    )
    codes = [r[0] for r in _run(con, "SELECT code FROM sales.customers", gov)]
    shapes = re.compile(r"^([A-Z]{2}-[0-9]{2}|[a-z]\.[0-9]@x%!)$")
    assert all(shapes.match(c) for c in codes), [c for c in codes if not shapes.match(c)][:5]
    assert {len(c) for c in codes} == {5, 7}  # both shapes are drawn
    assert len(set(codes)) > 100  # their letters and digits are drawn, not fixed


def test_a_relative_fake_with_no_distance_moves_by_the_measured_difference(con):
    gov = _gov(
        {**_FAKES, "shipped": "after(created)"},
        measured={"shipped": Measured(points=((0.0, 86400.0), (1.0, 172800.0)))},
    )
    for created, shipped in _run(con, "SELECT created, shipped FROM sales.customers", gov):
        assert dt.timedelta(days=1) <= shipped - created <= dt.timedelta(days=2)


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


def test_a_stable_fake_is_the_portable_definition_s_at_its_pinned_version(con):
    from provisa.fakes.digest import definition_hash, digest
    from provisa.fakes.portable import stable_fake

    gov = _gov({"email": "email()", "tier": "name()"}, stable={"email", "tier"}, stable_version=1)
    rows = _run(con, "SELECT id, email, tier FROM sales.customers ORDER BY id", gov)
    for i, email, tier in rows[:20]:
        d = digest(_KEY, f"user{i % 150}@real.example")
        local, domain = stable_fake(
            "email", 1, d, definition_hash("email", {}, 1, "varchar")
        ).split("@")
        assert email.startswith(local + ".") and email.endswith("@" + domain)  # tagged, as ever
        assert tier == stable_fake(
            "name", 1, digest(_KEY, "bronze"), definition_hash("name", {}, 1, "varchar")
        )


def test_two_columns_faked_from_one_value_draw_independently(con):
    """REQ-1494: every fake is seeded by the value's digest combined with its definition's hash,
    so score and twin -- one real value each row -- faked by two distributions are not one
    function of the other."""
    gov = _gov({"score": "uniform(min=0, max=1)", "twin": "uniform(min=0, max=2)"})
    rows = _run(con, "SELECT score, twin FROM sales.customers", gov)
    assert sum(1 for a, b in rows if abs(b - 2 * a) < 1e-9) < 5
    same = _gov({"score": "uniform(min=0, max=1)", "twin": "uniform(min=0, max=1)"})
    assert all(a == b for a, b in _run(con, "SELECT score, twin FROM sales.customers", same))


def test_a_definition_change_moves_a_columns_values(con):
    first = dict(
        _run(con, "SELECT id, score FROM sales.customers", _gov({"score": "uniform(min=0, max=1)"}))
    )
    wider = dict(
        _run(con, "SELECT id, score FROM sales.customers", _gov({"score": "uniform(min=0, max=2)"}))
    )
    assert sum(1 for i in first if abs(wider[i] - 2 * first[i]) < 1e-9) < 5
    hashed = dict(_run(con, "SELECT id, email FROM sales.customers", _gov({"email": "hash()"})))
    stable = _gov({"email": "hash()"}, stable={"email"}, stable_version=1)
    pinned = dict(_run(con, "SELECT id, email FROM sales.customers", stable))
    assert all(hashed[i] != pinned[i] for i in hashed)
