# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8f6c14-d9a3-4e72-81c5-a4e0d7b3f926
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Checks a profile hands to a Soda or Great Expectations checker (REQ-1934): an accepted
constraint, the drift check and an expectation check, each written by the one contract builder and
refused by name where no checker can take it."""

from __future__ import annotations

import json

import pytest
import yaml

from provisa.dq.contract import build_contract, contract_checks
from provisa.profiler import export
from provisa.profiler.constraints import Constraint

_SODA = export.CheckerTable(
    1,
    "orders_checks",
    "soda_src",
    "soda",
    "provisa/sales/orders",
    build_contract("soda", "provisa/sales/orders", []),
)
_GX = export.CheckerTable(
    2,
    "orders_gx",
    "gx_src",
    "great_expectations",
    "provisa/sales/orders",
    build_contract("great_expectations", "provisa/sales/orders", []),
)

_CONSTRAINTS = {
    "not_null": Constraint("not_null", "id", None, {}),
    "unique": Constraint("unique", "id", None, {}),
    "value_set": Constraint("value_set", "status", None, {"values": ["new", "paid"]}),
    "range": Constraint("range", "amount", None, {"min": 0, "max": 99, "family": "numeric"}),
    "dates": Constraint(
        "range",
        "placed",
        None,
        {
            "min": 0,
            "max": 1,
            "min_text": "2024-01-01",
            "max_text": "2024-12-31",
            "family": "temporal",
        },
    ),
    "ordering": Constraint("ordering", "placed", "shipped", {"family": "temporal"}),
}


@pytest.mark.parametrize("checker", [_SODA, _GX], ids=["soda", "gx"])
def test_every_constraint_kind_becomes_a_check_the_contract_builder_accepts(checker):
    rows = [export.constraint_check(c, checker.checker) for c in _CONSTRAINTS.values()]
    text = build_contract(checker.checker, checker.dataset, rows)
    assert len(contract_checks(text, checker.checker)) == len(rows)


def test_soda_checks_are_soda_column_checks_and_failed_rows_sql():
    by = {k: export.constraint_check(c, "soda") for k, c in _CONSTRAINTS.items()}
    assert (by["not_null"]["check_type"], by["not_null"]["column_name"]) == ("missing", "id")
    assert by["unique"]["check_type"] == "duplicate"
    assert yaml.safe_load(by["value_set"]["definition"]) == {"valid_values": ["new", "paid"]}
    assert yaml.safe_load(by["range"]["definition"]) == {"valid_min": 0, "valid_max": 99}
    assert yaml.safe_load(by["ordering"]["definition"]) == {"expression": '"placed" > "shipped"'}
    assert (
        "CAST('2024-12-31' AS TIMESTAMP)" in yaml.safe_load(by["dates"]["definition"])["expression"]
    )


def test_gx_ordering_is_the_column_pair_expectation():
    row = export.constraint_check(_CONSTRAINTS["ordering"], "great_expectations")
    assert row["check_type"] == "expect_column_pair_values_a_to_be_greater_than_b"
    assert json.loads(row["definition"]) == {
        "column_A": "shipped",
        "column_B": "placed",
        "or_equal": True,
    }


def test_the_drift_check_finds_drifting_measures_of_the_latest_run():
    soda = yaml.safe_load(export.drift_check(_SODA)["definition"])["query"]
    assert soda == (
        'SELECT * FROM "sales"."orders" WHERE drifting = TRUE AND run_time = '
        '(SELECT MAX(run_time) FROM "sales"."orders")'
    )
    gx = json.loads(export.drift_check(_GX)["definition"])["unexpected_rows_query"]
    assert gx.startswith("SELECT * FROM {batch} WHERE drifting = TRUE")


def test_the_expectation_check_joins_the_expectation_whose_period_holds_the_run():
    query = yaml.safe_load(export.expectation_check(_SODA, "plan.targets")["definition"])["query"]
    assert 'JOIN "plan"."targets" e ON e."measure" = d.measure' in query
    assert 'd.run_time >= e."period_start" AND d.run_time < e."period_end"' in query
    assert 'd."current" < e."low" OR d."current" > e."high"' in query


def test_a_check_with_no_checker_to_take_it_is_refused_by_name():
    with pytest.raises(
        export.ExportRefused,
        match="no Soda or Great Expectations checker source scans table 'orders'",
    ):
        export.pick([], None, "table 'orders'")
    with pytest.raises(export.ExportRefused, match="pick one"):
        export.pick([_SODA, _GX], None, "table 'orders'")
    with pytest.raises(export.ExportRefused, match="checker table 9 does not scan"):
        export.pick([_SODA], 9, "table 'orders'")
    assert export.pick([_SODA, _GX], 2, "table 'orders'") is _GX


def test_the_expectations_shape():
    assert export.is_expectations_shape(list(export.EXPECTATION_SHAPE) + ["note"]) == []
    assert export.is_expectations_shape(["measure", "low", "high"]) == [
        "column",
        "period_start",
        "period_end",
        "expected",
        "source",
    ]


async def test_adding_a_check_rewrites_the_contract_once():
    class _Conn:
        def __init__(self):
            self.statements = []

        async def execute_core(self, stmt):
            self.statements.append(stmt)

    conn = _Conn()
    row = export.constraint_check(_CONSTRAINTS["unique"], "soda")
    assert await export.add_check(conn, _SODA, row) is True
    written = conn.statements[0].compile().params["dq_contract"]
    assert [c["check_type"] for c in contract_checks(written, "soda")] == ["duplicate"]
    again = export.CheckerTable(1, "orders_checks", "soda_src", "soda", _SODA.dataset, written)
    assert await export.add_check(conn, again, row) is False
    assert len(conn.statements) == 1
