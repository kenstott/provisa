# Copyright (c) 2026 Kenneth Stott
# Canary: 0e6b3f58-a1d7-4c92-8b34-f5c7d2e9a160
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Declared profiles (REQ-1942): hand-written profile facts, stored as a profile run's result rows,
refused by name where a column has nothing to generate it from, and copied back out of a run."""

# Requirements: REQ-1942

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from provisa.profiler.declared import (
    DECLARED,
    DeclaredProfileRefused,
    as_declared,
    results,
)
from provisa.profiler.schema import field_names
from provisa.profiler.statement import QUANTILE_POINTS, ColumnSpec, FanoutSpec

_WHEN = datetime(2026, 10, 7, tzinfo=UTC)


def _target() -> SimpleNamespace:
    return SimpleNamespace(
        table_id=7,
        table_name="orders",
        columns=[
            ColumnSpec("id", "integer", "numeric", "id"),
            ColumnSpec("amount", "integer", "numeric", "amount"),
            ColumnSpec("placed", "timestamp", "temporal", "placed"),
            ColumnSpec("region", "varchar", "text", "region"),
            ColumnSpec("note", "varchar", "text", "note"),
        ],
        fanouts=[FanoutSpec("lines", "sales.lines", "id", "order_id")],
        parents=[],
    )


def _doc(**extra) -> dict:
    return {
        "rowCount": 1000,
        "columns": {
            "amount": {"nullShare": 0.1, "distinctCount": 90, "range": {"min": 10, "max": 100}},
            "placed": {
                "nullShare": 0,
                "distinctCount": 365,
                "range": {"min": "2025-01-01", "max": "2026-01-01"},
            },
            "region": {
                "nullShare": 0,
                "values": [{"value": "east", "weight": 3}, {"value": "west", "weight": 1}],
            },
        },
        "fanouts": {"lines": {"range": {"min": 0, "max": 4}}},
        **extra,
    }


def _rows(doc: dict, covered=frozenset({"id", "note"})) -> dict[str, list[dict]]:
    return results(_target(), doc, covered, "declared-1", _WHEN)


def test_a_declared_profile_is_a_succeeded_run_marked_declared():
    out = _rows(_doc())
    (run,) = out["runs"]
    assert run["status"] == "succeeded" and run["sample_method"] == DECLARED
    assert run["row_count"] == run["profiled_rows"] == 1000
    for kind, rows in out.items():
        assert all(tuple(r) == field_names(kind) for r in rows), kind


def test_a_range_is_a_uniform_quantile_sketch_and_shares_are_counts():
    out = _rows(_doc())
    amount = [q["value"] for q in out["quantiles"] if q["column_name"] == "amount"]
    assert len(amount) == len(QUANTILE_POINTS) and amount[0] == 10 and amount[-1] == 100
    placed = [q["value"] for q in out["quantiles"] if q["column_name"] == "placed"]
    assert placed[0] == datetime(2025, 1, 1, tzinfo=UTC).timestamp()
    cols = {c["column_name"]: c for c in out["columns"]}
    assert cols["amount"]["null_count"] == 100 and cols["amount"]["integer_only"] is True
    assert cols["region"]["distinct_count"] == 2
    top = {v["value"]: v["row_count"] for v in out["top_values"]}
    assert top == {"east": 750, "west": 250}
    (fan,) = out["fanout_runs"]
    assert fan["child_table"] == "sales.lines" and fan["parents"] == 1000


def test_a_column_with_no_fact_fake_or_rule_is_refused_by_name():
    with pytest.raises(DeclaredProfileRefused, match=r"orders\.note"):
        _rows(_doc(), covered=frozenset({"id"}))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"rowCount": -1}, "rowCount"),
        ({"columns": {"nope": {"nullShare": 0, "distinctCount": 1}}}, "no column 'nope'"),
        ({"columns": {"amount": {"distinctCount": 3, "range": {"min": 1, "max": 2}}}}, "nullShare"),
        ({"columns": {"amount": {"nullShare": 2, "distinctCount": 1}}}, "share from 0 to 1"),
        ({"columns": {"amount": {"nullShare": 0, "distinctCount": 1}}}, "no values, range"),
        ({"columns": {"amount": {"nullShare": 0, "range": {"min": 1, "max": 2}}}}, "distinctCount"),
        (
            {
                "columns": {
                    "amount": {"nullShare": 0, "distinctCount": 1, "range": {"min": 2, "max": 1}}
                }
            },
            "max is below min",
        ),
        (
            {
                "columns": {
                    "region": {
                        "nullShare": 0,
                        "distinctCount": 1,
                        "range": {"min": "a", "max": "b"},
                    }
                }
            },
            "no range",
        ),
        ({"fanouts": {"nope": {"range": {"min": 0, "max": 1}}}}, "no one-to-many relationship"),
        ({"dependence": {"correlations": [{"column_name": "amount"}]}}, "each row holds exactly"),
    ],
)
def test_a_malformed_fact_is_refused_saying_what_is_wrong(change, message):
    doc = _doc()
    for key, value in change.items():
        if key == "columns":
            doc["columns"] = {**doc["columns"], **value}
        else:
            doc[key] = value
    covered = frozenset({"id", "note", "amount", "placed", "region"})
    with pytest.raises(DeclaredProfileRefused, match=message):
        _rows(doc, covered)


def test_a_run_copied_into_a_declared_profile_stores_the_same_facts():
    out = _rows(_doc())
    doc = as_declared(out)
    assert doc["rowCount"] == 1000
    assert doc["columns"]["region"]["values"] == [
        {"value": "east", "weight": 750},
        {"value": "west", "weight": 250},
    ]
    assert len(doc["columns"]["amount"]["quantiles"]) == len(QUANTILE_POINTS)
    again = _rows(doc)
    assert [q["value"] for q in again["quantiles"]] == [q["value"] for q in out["quantiles"]]
    assert {(v["value"], v["row_count"]) for v in again["top_values"]} == {
        ("east", 750),
        ("west", 250),
    }
    # A what-if: ten times the orders, the same mix.
    doc["rowCount"] = 10_000
    assert _rows(doc)["runs"][0]["row_count"] == 10_000


def test_a_column_shown_by_its_shape_only_is_left_out_of_the_copy():
    out = _rows(_doc())
    for c in out["columns"]:
        if c["column_name"] == "region":
            c["null_share"] = None  # governance.safe_run on a restricted column
    assert "region" not in as_declared(out)["columns"]


def test_a_run_that_read_no_rows_has_nothing_to_copy():
    out = _rows(_doc(rowCount=0))
    with pytest.raises(DeclaredProfileRefused, match="read no rows"):
        as_declared(out)
