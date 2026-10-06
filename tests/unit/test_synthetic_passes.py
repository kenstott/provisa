# Copyright (c) 2026 Kenneth Stott
# Canary: fa1110b4-4a6d-4e13-818b-bfde265c59d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generation in passes (REQ-1939, REQ-1494): row-spanning synthetic rules computed over the
generated rows -- a sequence's states in order per entity, its entity holding no more rows than
states; an sql_group window over the table naming the drawn value as self; an sql_group aggregate
over a parent's children computed in the second pass over the generated child table; and the
fakes that read a rule's column computed after it. Run on an in-process DuckDB."""

from __future__ import annotations

from collections import defaultdict

import duckdb
import pytest

from provisa.fakes.duckdb_functions import register
from provisa.fakes.kinds import parse
from provisa.synthetic.generate import children_count_sql, generation_sql
from provisa.synthetic.group import ChildLink
from provisa.synthetic.plan import (
    DatasetRefused,
    DatasetTable,
    Edge,
    ProfiledColumn,
    ProfiledFanout,
    ProfiledTable,
    plan_tables,
)

_COUNTER = duckdb.connect()
# Children per parent: 0 to 8, evenly.
_FANOUT = tuple(float(i * 8 // 100) for i in range(101))


def _col(physical: str, family: str = "numeric", **kw) -> ProfiledColumn:
    base = dict(
        physical=physical,
        family=family,
        null_count=0,
        distinct_count=900,
        distinct_ratio=0.9,
        integer_only=True,
        min_value="1",
        sketch=tuple(float(i) for i in range(101)),
        top=(),
        frequencies=(),
        shapes=(),
    )
    base.update(kw)
    return ProfiledColumn(**base)


def _customers(fakes=None, children=None) -> DatasetTable:
    return DatasetTable(
        table_id=1,
        name="customers",
        pgwire_name="sales.customers",
        columns=(("id", "integer", True), ("total", "double", False)),
        scale=1.0,
        fakes=fakes or {},
        profile=ProfiledTable(
            "r1",
            200,
            200,
            {"id": _col("id"), "total": _col("total")},
            (ProfiledFanout("sales.purchases", 200, _FANOUT),),
        ),
        children=children or {},
    )


def _purchases(fakes, self_fakes=None) -> DatasetTable:
    return DatasetTable(
        table_id=2,
        name="purchases",
        pgwire_name="sales.purchases",
        columns=(
            ("id", "integer", True),
            ("customer_id", "integer", False),
            ("amount", "double", False),
            ("running", "double", False),
            ("status", "varchar", False),
            ("label", "varchar", False),
        ),
        scale=1.0,
        fakes=fakes,
        self_fakes=self_fakes or {},
        profile=ProfiledTable(
            "r2",
            800,
            800,
            {
                "id": _col("id", min_value="1000"),
                "customer_id": _col("customer_id"),
                "amount": _col("amount"),
                "running": _col("running"),
                "status": _col("status", "text", shapes=(("aaaa", 1),)),
                "label": _col("label", "text", shapes=(("aaaa", 1),)),
            },
            (),
        ),
    )


_EDGES = [Edge(1, "id", 2, "customer_id")]
_NAMES = {1: "customers", 2: "purchases"}


def _count(plan) -> int:
    return _COUNTER.execute(children_count_sql(plan, "duckdb")).fetchone()[0]


def _unmeasured(t, column):
    raise AssertionError(f"{t.name}.{column} was read from its table")


@pytest.fixture
def con():
    c = duckdb.connect()
    register(c)
    return c


def _plan(tables):
    return {
        p.table.name: p
        for p in plan_tables(
            tables, _EDGES, seed=3, names=_NAMES, count_rows=_count, measure=_unmeasured
        )
    }


def _rows(con, sql: str) -> list[dict]:
    res = con.execute(sql)
    names = [d[0] for d in res.description]
    return [dict(zip(names, r)) for r in res.fetchall()]


_PURCHASE_RULES = {
    "status": parse("sequence((new, paid, shipped), customer_id, id)", rule=True),
    "amount": parse("sql(1)"),
    "running": parse(
        "sql_group(SUM(amount) OVER (PARTITION BY customer_id ORDER BY id))", rule=True
    ),
    "label": parse("sql(status || '-' || CAST(running AS VARCHAR))"),
}


def test_a_sequence_takes_its_states_in_order_and_caps_each_entity(con):
    planned = _plan([_customers(), _purchases(_PURCHASE_RULES)])
    assert planned["purchases"].plan.cap == 3
    rows = _rows(con, generation_sql(planned["purchases"].plan, "duckdb"))
    by_customer = defaultdict(list)
    for r in sorted(rows, key=lambda r: r["id"]):
        by_customer[r["customer_id"]].append(r)
    assert by_customer and all(len(v) <= 3 for v in by_customer.values())
    for rs in by_customer.values():
        assert [r["status"] for r in rs] == ["new", "paid", "shipped"][: len(rs)]
        # A window naming the drawn values: each customer's running count of purchases.
        assert [r["running"] for r in rs] == [float(i) for i in range(1, len(rs) + 1)]
        # A fake reading a rule's column is computed after it.
        assert [r["label"] for r in rs] == [f"{r['status']}-{r['running']}" for r in rs]


@pytest.mark.parametrize(
    "rule", ["sql_group(SUM(purchases.amount))", "sql_group(SUM(purchases.amount) + self * 0)"]
)
def test_a_rule_over_children_is_computed_in_the_second_pass(con, rule):
    link = ChildLink("purchases", "purchases", "id", "customer_id")
    customers = _customers({"total": parse(rule, rule=True)}, children={"purchases": link})
    planned = _plan([customers, _purchases(_PURCHASE_RULES)])
    con.execute(
        f"CREATE TABLE gen_purchases AS {generation_sql(planned['purchases'].plan, 'duckdb')}"
    )
    first = _rows(con, generation_sql(planned["customers"].plan, "duckdb"))
    second = _rows(
        con,
        generation_sql(
            planned["customers"].plan, "duckdb", child_tables={"purchases": '"gen_purchases"'}
        ),
    )
    assert sorted(r["id"] for r in first) == sorted(r["id"] for r in second)  # the same rows
    counts = dict(
        con.execute("SELECT customer_id, SUM(amount) FROM gen_purchases GROUP BY 1").fetchall()
    )
    for r in second:
        assert r["total"] == counts.get(r["id"])  # NULL for a customer with no purchases


def test_a_sequence_whose_entity_is_not_the_driving_relationship_is_refused():
    rules = {"status": parse("sequence((new, paid), label, id)", rule=True)}
    with pytest.raises(DatasetRefused, match="a sequence's entity 'label' must be the column"):
        _plan([_customers(), _purchases(rules)])


def test_a_rule_reading_a_child_the_dataset_does_not_generate_is_refused():
    link = ChildLink("lines", "order_lines", "id", "order_id")
    customers = _customers(
        {"total": parse("sql_group(SUM(lines.qty))", rule=True)}, children={"lines": link}
    )
    with pytest.raises(DatasetRefused, match="whose table 'order_lines' the dataset does not"):
        _plan([customers, _purchases({})])


def test_the_trino_statements_of_both_passes_parse():
    import sqlglot

    link = ChildLink("purchases", "purchases", "id", "customer_id")
    customers = _customers(
        {"total": parse("sql_group(SUM(purchases.amount))", rule=True)},
        children={"purchases": link},
    )
    planned = _plan([customers, _purchases(_PURCHASE_RULES)])
    sqlglot.parse_one(generation_sql(planned["purchases"].plan, "trino"), read="trino")
    second = generation_sql(
        planned["customers"].plan, "trino", child_tables={"purchases": '"store"."ds"."p"'}
    )
    sqlglot.parse_one(second, read="trino")
    assert '"store"."ds"."p"' in second
