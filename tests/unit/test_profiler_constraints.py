# Copyright (c) 2026 Kenneth Stott
# Canary: 0e5c3a87-61b9-4d24-a7f2-d8b4e1c9f630
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Constraints a profile proposes and the accepted ones every run checks (REQ-1934 PROPOSED
CONSTRAINTS), the statements run against an in-process DuckDB as the pipeline transpiles them."""

from __future__ import annotations

import json

import duckdb
import pytest
import sqlglot

from provisa.profiler import constraints as pc
from provisa.profiler import dependence as dep
from provisa.profiler.statement import ColumnSpec, Sample, parse_profile_result, profile_sql

_COLUMNS = [
    ColumnSpec("id", "integer", "numeric", "id"),
    ColumnSpec("status", "varchar", "text", "status"),
    ColumnSpec("placed", "date", "temporal", "placed"),
    ColumnSpec("shipped", "date", "temporal", "shipped"),
    ColumnSpec("note", "varchar", "text", "note"),
]


@pytest.fixture
def con():
    db = duckdb.connect()
    db.execute("CREATE SCHEMA d")
    # id unique and never null; status one of three; shipped never before placed; note half null.
    db.execute(
        "CREATE TABLE d.orders AS SELECT range::INTEGER AS id, "
        "CASE range % 3 WHEN 0 THEN 'new' WHEN 1 THEN 'paid' ELSE 'sent' END AS status, "
        "DATE '2024-01-01' + (range::INTEGER) AS placed, "
        "DATE '2024-01-01' + (range::INTEGER) + (range % 4)::INTEGER AS shipped, "
        "CASE WHEN range % 2 = 0 THEN NULL ELSE 'x' || range::VARCHAR END AS note "
        "FROM range(300)"
    )
    return db


def _run(con, sql: str):
    res = con.execute(sqlglot.transpile(sql, read="postgres", write="duckdb")[0])
    return [d[0] for d in res.description], res.fetchall()


def _profile(con, checks=()):
    checks = list(checks)
    names, rows = _run(con, profile_sql("d.orders", _COLUMNS, [], Sample("whole"), 100, [], checks))
    return parse_profile_result(names, rows, _COLUMNS, [], [], checks)


def _proposed(agg) -> dict[tuple, pc.Proposal]:
    out = {}
    for col in agg.columns:
        for p in pc.propose_for_column(col, agg.profiled_rows, 100):
            out[(p.constraint.kind, p.constraint.column)] = p
    return out


def test_a_profile_proposes_what_its_evidence_supports(con):
    proposed = _proposed(_profile(con))
    assert proposed[("not_null", "id")].share == 1.0
    assert proposed[("unique", "id")].evidence == "0 of 300 non-null rows repeat a value"
    assert ("not_null", "note") not in proposed  # half its rows are null
    assert proposed[("value_set", "status")].constraint.definition["values"] == [
        "new",
        "paid",
        "sent",
    ]
    assert ("unique", "status") not in proposed
    rng = proposed[("range", "id")].constraint.definition
    assert (rng["min"], rng["max"]) == (0.0, 299.0)
    assert proposed[("range", "placed")].constraint.definition["min_text"] == "2024-01-01"


def test_an_ordering_is_proposed_from_the_row_by_row_comparison(con):
    own = list(zip(_COLUMNS, [300, 3, 300, 300, 150]))
    cols = dep.choose_columns(own, [], max_numbers=20, max_distinct=20)
    pairs = dep.parse_pairs(
        *_run(con, dep.pairs_sql("d.orders", _COLUMNS, [], cols, Sample("whole"))), cols
    )
    found = {(p.constraint.column, p.constraint.other) for p in pc.propose_orderings(pairs, cols)}
    assert ("placed", "shipped") in found
    # id and placed are numbers of different families: never compared.
    assert not any("id" in pair for pair in found)


def test_every_run_checks_the_accepted_constraints(con):
    accepted = [
        pc.AcceptedConstraint(str(i), c, "evidence", 1.0, False)
        for i, c in enumerate(
            [
                pc.Constraint("not_null", "note", None, {}),
                pc.Constraint("unique", "status", None, {}),
                pc.Constraint("value_set", "status", None, {"values": ["new", "paid"]}),
                pc.Constraint("range", "id", None, {"min": 0, "max": 199}),
                pc.Constraint("ordering", "shipped", "placed", {"family": "temporal"}),
                pc.Constraint("not_null", "gone", None, {}),
            ]
        )
    ]
    readable = accepted[:-1]
    agg = _profile(con, [c.constraint.check() for c in readable])
    rows = {r["constraint_id"]: r for r in pc.check_rows(accepted, readable, agg)}
    assert rows["0"]["violations"] == 150 and rows["0"]["pass_share"] == 0.5
    assert rows["1"]["violations"] == 297  # three values over 300 rows: 297 repeat one
    assert rows["2"]["violations"] == 100  # 'sent'
    assert rows["3"]["violations"] == 100  # ids 200..299
    # shipped is after placed on 3 of every 4 rows.
    assert rows["4"]["violations"] == 225
    assert (
        rows["5"]["violations"] is None and "not one the org admin can read" in rows["5"]["detail"]
    )
    assert json.loads(rows["4"]["involved_columns"]) == ["shipped", "placed"]
    assert rows["2"]["value_bearing"] is True and rows["0"]["value_bearing"] is False


def test_proposal_rows_carry_the_operators_decision_and_the_sample():
    proposal = pc.Proposal(pc.Constraint("not_null", "id", None, {}), "0 of 3 null", 1.0)
    (row,) = pc.proposal_rows([proposal], {"not_null|id|": "dismissed"}, sampled=True)
    assert (row["status"], row["sampled"], row["constraint"]) == ("dismissed", True, "not_null")


def test_a_constraint_is_one_of_the_kinds_and_only_an_ordering_names_two_columns():
    with pytest.raises(ValueError, match="unknown constraint kind"):
        pc.Constraint("positive", "id", None, {})
    with pytest.raises(ValueError, match="names a second column"):
        pc.Constraint("ordering", "a", None, {})
    with pytest.raises(ValueError, match="names a second column"):
        pc.Constraint("not_null", "a", "b", {})
