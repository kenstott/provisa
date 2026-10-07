# Copyright (c) 2026 Kenneth Stott
# Canary: d5a83ed4-2994-48e1-881d-172ca3309c77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Not too close to a real row (REQ-1939; maintainer rulings W1, Z2, C1): the redraw, the
selection and the cascade run on an in-process DuckDB, and the report's measures."""

# Requirements: REQ-1939

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import duckdb
import pytest

from provisa.synthetic import closeness
from provisa.synthetic.generate import (
    Closeness,
    ColumnPlan,
    DistanceColumn,
    ForeignKey,
    PlannedCondition,
    TablePlan,
    closeness_counts_sql,
    closeness_sql,
    distance_columns,
    generated_nearest_sql,
    generation_sql,
    sample_nearest_sql,
)

_KEY = ColumnPlan("id", "BIGINT", "numeric", key=True)
_AMOUNT = ColumnPlan("amount", "DOUBLE", "numeric", sketch=tuple(float(i) for i in range(101)))
_REGION = ColumnPlan(
    "region", "VARCHAR", "text", frequencies=(("east", 0.5), ("west", 0.3), ("north", 0.2))
)
_CUSTOMERS = TablePlan("customers", 40, (_KEY, _AMOUNT, _REGION), seed=7)
_COLUMNS = (
    DistanceColumn("amount", "numeric", 50.0, "DOUBLE"),
    DistanceColumn("region", "text", None, "VARCHAR"),
)


def _orders(parent: TablePlan | None = None) -> TablePlan:
    return TablePlan(
        "orders",
        0,
        (
            _KEY,
            ColumnPlan(
                "customer_id",
                "BIGINT",
                "numeric",
                foreign_key=ForeignKey("customers", 40, _KEY),
                driving=True,
            ),
            ColumnPlan("total", "DOUBLE", "numeric", sketch=tuple(float(i) for i in range(101))),
        ),
        fanout=(40, tuple(2.0 for _ in range(101)), ()),
        seed=7,
        parent=parent,
        parent_key="id" if parent is not None else None,
        conditions=(PlannedCondition("region = 'east'", fixed=3),) if parent else (),
    )


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


def _sample_of(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    """The real sample as a landed one holds it: an id, then each column as the distance reads it."""
    con.execute('CREATE OR REPLACE TABLE sample ("__sid" BIGINT, "__d0" DOUBLE, "__d1" VARCHAR)')
    con.executemany("INSERT INTO sample VALUES (?, ?, ?)", [(i, *r) for i, r in enumerate(rows)])


def _nearest(row: tuple, sample: list[tuple]) -> float:
    def d(a: tuple, b: tuple) -> float:
        return (abs(a[0] - b[0]) / 50.0 + (0.0 if a[1] == b[1] else 1.0)) / 2

    return min(d(row, s) for s in sample)


# -- the measures -------------------------------------------------------------------------------


def test_quantile_interpolates_between_values():
    assert closeness.quantile([4.0, 1.0, 3.0, 2.0], 0.5) == 2.5
    assert closeness.quantile([1.0], 0.5) == 1.0


def test_a_number_without_spread_is_compared_as_equal_or_not():
    assert closeness.scale_of([5.0, 5.0, 5.0, 5.0]) is None
    assert closeness.scale_of([1.0]) is None
    assert closeness.scale_of([0.0, 1.0, 2.0, 3.0, 4.0]) == 2.0


def test_the_threshold_is_its_share_of_the_real_rows_median_nearest_distance():
    assert closeness.threshold(0.5, [1.0, 2.0, 3.0]) == 1.0


def test_membership_auc_is_a_half_when_generated_rows_lie_as_real_ones_do():
    assert closeness.membership_auc([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == 0.5
    assert closeness.membership_auc([0.0, 0.0], [1.0, 2.0]) == 1.0
    assert closeness.membership_auc([5.0, 6.0], [1.0, 2.0]) == 0.0


def test_a_ratio_is_left_out_where_the_second_nearest_is_at_zero():
    assert closeness.ratios([(1.0, 2.0), (0.0, 0.0), (1.0, 4.0)]) == ([0.5, 0.25], 1)


def test_the_report_says_closeness_is_not_checked_when_the_dataset_declares_none():
    (entry,) = closeness.not_checked()
    assert entry["measure"] == "closeness"
    assert entry["note"].startswith("not checked")


def test_the_report_states_threshold_counts_distances_ratio_and_membership():
    entries = closeness.report_entries(
        "customers",
        share=0.5,
        limit=0.1,
        draws=3,
        counts=(4, 2, 1),
        real=[(0.2, 0.3), (0.4, 0.5), (0.2, 0.4)],
        generated=[(0.3, 0.5), (0.25, 0.5)],
    )
    by = {}
    for e in entries:
        by.setdefault(e["measure"], []).append(e)
    assert [e["synthetic_value"] for e in by["closeness_redrawn"]] == [4.0]
    assert [e["synthetic_value"] for e in by["closeness_dropped"]] == [2.0]
    assert [e["synthetic_value"] for e in by["closeness_cascaded"]] == [1.0]
    assert by["closeness_threshold"][0]["source_value"] == 0.2
    assert len(by["closeness_distance"]) == len(closeness.REPORT_QUANTILES)
    assert by["closeness_nndr"][0]["synthetic_value"] == pytest.approx(0.55)
    assert by["closeness_membership_auc"][0]["synthetic_value"] == pytest.approx(2 / 6)


# -- the draws ----------------------------------------------------------------------------------


def test_a_later_draw_redraws_values_but_keeps_the_key_and_fixed_columns():
    con = duckdb.connect()
    first = _rows(con, f"SELECT id, amount, region FROM ({generation_sql(_CUSTOMERS, 'duckdb')})")
    again = _rows(
        con,
        f"SELECT id, amount, region FROM ({generation_sql(replace(_CUSTOMERS, draw=1), 'duckdb')})",
    )
    assert [r[0] for r in first] == [r[0] for r in again]
    assert [r[1] for r in first] != [r[1] for r in again]
    held = replace(_CUSTOMERS, draw=1, fixed=frozenset({"region"}))
    fixed = _rows(con, f"SELECT region FROM ({generation_sql(held, 'duckdb')})")
    assert [r[2] for r in first] == [r[0] for r in fixed]


def test_a_draw_keeps_the_rows_children_counts_and_parents():
    con = duckdb.connect()
    plan = _orders()
    first = _rows(con, f"SELECT id, customer_id FROM ({generation_sql(plan, 'duckdb')})")
    again = _rows(
        con, f"SELECT id, customer_id FROM ({generation_sql(replace(plan, draw=2), 'duckdb')})"
    )
    assert first == again


def test_the_distance_reads_only_columns_drawn_from_real_values():
    names = [c.name for c in distance_columns(_orders())]
    assert names == ["total"]


def test_the_distance_leaves_out_a_column_a_rule_spanning_rows_decides():
    from provisa.fakes.kinds import parse
    from provisa.synthetic.group import GroupColumn

    rule = GroupColumn("amount", "DOUBLE", parse("sql_group(SUM(1))", rule=True), ())
    plan = replace(_CUSTOMERS, group=(rule,))
    assert [c.name for c in distance_columns(plan)] == ["region"]


def test_fixed_columns_are_those_a_childs_conditions_read():
    planned = [SimpleNamespace(plan=_CUSTOMERS), SimpleNamespace(plan=_orders(_CUSTOMERS))]
    assert closeness.fixed_columns(planned) == {"customers": frozenset({"region"})}


# -- the selection ------------------------------------------------------------------------------


def test_every_kept_row_is_at_least_the_threshold_from_every_real_row():
    con = duckdb.connect()
    real = [(float(a), r) for a, r in zip(range(0, 100, 7), ["east", "west", "north"] * 5)]
    _sample_of(con, real)
    close = Closeness("sample", 0.06, 3, _COLUMNS)
    kept = _rows(con, f"SELECT amount, region FROM ({closeness_sql(_CUSTOMERS, 'duckdb', close)})")
    assert kept
    assert all(_nearest(r, real) >= 0.06 for r in kept)
    redrawn, dropped, cascaded = _rows(con, closeness_counts_sql(_CUSTOMERS, "duckdb", close))[0]
    assert len(kept) + dropped == _CUSTOMERS.rows
    assert cascaded == 0
    first = _rows(con, f"SELECT amount, region FROM ({generation_sql(_CUSTOMERS, 'duckdb')})")
    too_near = sum(1 for r in first if _nearest(r, real) < 0.06)
    assert redrawn + dropped == too_near
    assert redrawn > 0 and too_near < _CUSTOMERS.rows


def test_a_row_copying_a_real_row_is_drawn_again():
    con = duckdb.connect()
    first = _rows(con, f"SELECT amount, region FROM ({generation_sql(_CUSTOMERS, 'duckdb')})")
    _sample_of(con, first)  # every first draw is a real row
    close = Closeness("sample", 1e-9, 2, _COLUMNS)
    kept = _rows(con, f"SELECT amount, region FROM ({closeness_sql(_CUSTOMERS, 'duckdb', close)})")
    redrawn, dropped, _cascaded = _rows(con, closeness_counts_sql(_CUSTOMERS, "duckdb", close))[0]
    assert redrawn == len(kept)
    assert redrawn + dropped == _CUSTOMERS.rows
    assert not set(kept) & set(first)


def test_a_row_with_no_draw_far_enough_is_dropped():
    con = duckdb.connect()
    _sample_of(con, [(50.0, "east"), (10.0, "west")])
    close = Closeness("sample", 10.0, 2, _COLUMNS)  # no row is that far from both
    assert _rows(con, closeness_sql(_CUSTOMERS, "duckdb", close)) == []
    assert _rows(con, closeness_counts_sql(_CUSTOMERS, "duckdb", close)) == [(0, 40, 0)]


def test_a_dropped_rows_children_drop_with_it():
    con = duckdb.connect()
    con.execute("CREATE TABLE kept_customers AS SELECT * FROM range(0, 40, 2) t(id)")
    close = Closeness("", 0.0, 1, (), (("customer_id", "kept_customers", "id"),))
    plan = _orders()
    kept = _rows(con, f"SELECT customer_id FROM ({closeness_sql(plan, 'duckdb', close)})")
    every = _rows(con, f"SELECT customer_id FROM ({generation_sql(plan, 'duckdb')})")
    assert kept and all(c % 2 == 0 for (c,) in kept)
    assert _rows(con, closeness_counts_sql(plan, "duckdb", close)) == [
        (0, 0, len(every) - len(kept))
    ]


def test_cascaded_rows_are_counted_apart_from_rows_too_near():
    con = duckdb.connect()
    con.execute("CREATE TABLE kept_customers AS SELECT * FROM range(0, 40, 2) t(id)")
    con.execute('CREATE TABLE sample ("__sid" BIGINT, "__d0" DOUBLE)')
    con.executemany("INSERT INTO sample VALUES (?, ?)", [(0, 1.0), (1, 99.0)])
    plan = _orders()
    close = Closeness(
        "sample",
        0.05,
        2,
        (DistanceColumn("total", "numeric", 50.0, "DOUBLE"),),
        (("customer_id", "kept_customers", "id"),),
    )
    kept = _rows(con, f"SELECT customer_id, total FROM ({closeness_sql(plan, 'duckdb', close)})")
    redrawn, dropped, cascaded = _rows(con, closeness_counts_sql(plan, "duckdb", close))[0]
    every = _rows(con, f"SELECT COUNT(*) FROM ({generation_sql(plan, 'duckdb')})")[0][0]
    assert len(kept) + dropped + cascaded == every
    assert all(c % 2 == 0 and min(abs(t - 1.0), abs(t - 99.0)) / 50.0 >= 0.05 for c, t in kept)


# -- the report's distances ---------------------------------------------------------------------


def test_nearest_distances_of_real_and_generated_rows():
    con = duckdb.connect()
    real = [(0.0, "east"), (10.0, "east"), (50.0, "west"), (100.0, "north")]
    _sample_of(con, real)
    close = Closeness("sample", 0.0, 1, _COLUMNS)
    pairs = sorted(_rows(con, sample_nearest_sql(close)))
    assert [(i, round(a, 6)) for i, a, _b in pairs] == [
        (0, 0.1),
        (1, 0.1),
        (2, round((40 / 50 + 1) / 2, 6)),
        (3, round((50 / 50 + 1) / 2, 6)),
    ]
    assert all(a <= b for _i, a, b in pairs)
    con.execute(f"CREATE TABLE generated AS {generation_sql(_CUSTOMERS, 'duckdb')}")
    got = _rows(con, generated_nearest_sql(_CUSTOMERS, "duckdb", close, "generated", 25, 7))
    assert len(got) == 25
    rows = _rows(con, "SELECT amount, region FROM generated")
    nearest = sorted(round(_nearest(r, real), 9) for r in rows)
    assert all(round(a, 9) in nearest and a <= b for _g, a, b in got)
    again = _rows(con, generated_nearest_sql(_CUSTOMERS, "duckdb", close, "generated", 25, 7))
    assert sorted(got) == sorted(again)


# -- the samples are dropped (W1) ----------------------------------------------------------------


async def test_the_real_samples_are_dropped_when_generation_fails(monkeypatch):
    from contextlib import asynccontextmanager

    from provisa.synthetic import run
    from provisa.synthetic.plan import PlannedTable

    row = SimpleNamespace(
        closeness_threshold=0.5,
        closeness_draws=2,
        private_epsilon=None,
        fanout_conditions=(),
        store_schema="s",
        seed=1,
        assertions=(),
        tables=(),
    )
    planned = [PlannedTable(table=SimpleNamespace(table_id=1), plan=TablePlan("t", 1, ()))]
    dropped: list = []

    async def get_dataset(conn, dataset_id):
        return row

    async def set_status(conn, dataset_id, status, **kw):
        return None

    async def dataset_tables(state, r):
        return [], [], {1: {"table_name": "t"}}

    async def pinned_runs(state, r, tables, registered):
        return {}

    async def addresses(state, schema, p, registered):
        return {"t": "addr"}

    async def closeness_of(state, r, p, registered, addr, landed):
        landed.append("__closeness__public__t")
        return {}, {}, []

    async def write_table(*a, **kw):
        raise RuntimeError("the store refused the write")

    async def drop_samples(state, schema, landed):
        dropped.append((schema, list(landed)))

    @asynccontextmanager
    async def acquire():
        yield None

    monkeypatch.setattr(run.datasets, "get_dataset", get_dataset)
    monkeypatch.setattr(run.datasets, "set_status", set_status)
    monkeypatch.setattr(run, "_require_non_prod", lambda env: "dev")
    monkeypatch.setattr(run, "_dataset_tables", dataset_tables)
    monkeypatch.setattr(run, "_pinned_runs", pinned_runs)
    monkeypatch.setattr(run, "plan_tables", lambda *a, **kw: planned)
    monkeypatch.setattr(run, "_generated_addresses", addresses)
    monkeypatch.setattr(run, "_closeness_of", closeness_of)
    monkeypatch.setattr(run, "_write_table", write_table)
    monkeypatch.setattr(run, "_drop_samples", drop_samples)
    state = SimpleNamespace(
        model_db=SimpleNamespace(acquire=acquire),
        _active_runtime=lambda: SimpleNamespace(data_mode="inherit"),
    )
    with pytest.raises(RuntimeError, match="refused the write"):
        await run.generate(state, "d")
    assert dropped == [("s", ["__closeness__public__t"])]


# -- the settings (Z2) ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("settings", "refusal"),
    [
        ({"closeness_threshold": 0.5}, "both a threshold and a number of draws, or neither"),
        ({"closeness_draws": 2}, "both a threshold and a number of draws, or neither"),
        ({"closeness_threshold": 0.0, "closeness_draws": 2}, "threshold must be above 0"),
        ({"closeness_threshold": 0.5, "closeness_draws": 0}, "at least one draw"),
        (
            {"closeness_threshold": 0.5, "closeness_draws": 2, "private_epsilon": 1.0},
            "declare ε or closeness, not both",
        ),
    ],
)
async def test_closeness_settings_are_both_or_neither_and_never_private(settings, refusal):
    from provisa.synthetic.datasets import DatasetTableRow, define

    with pytest.raises(ValueError, match=refusal):
        await define(
            None,
            dataset_id="close",
            seed=1,
            scale=1.0,
            store_schema="s",
            tables=[DatasetTableRow(table_id=1, profile_env="prod", run_id="r", scale=None)],
            **settings,
        )
