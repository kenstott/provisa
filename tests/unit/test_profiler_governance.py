# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1d6f39-2c75-4e04-b9a3-d5f0e7c2b816
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A profile's governance from the profiled table's current column rules (REQ-1934): the runs view
made safe for its viewer, and the rules a result table is registered with by default."""

from __future__ import annotations

from provisa.profiler.governance import ColumnRule, prefill, safe_run
from provisa.profiler.schema import field_names

_ALL = frozenset({"org_admin", "analyst", "auditor"})
RULES = [
    ColumnRule("id", "id", _ALL, frozenset(), masked=False, pii=False),
    # masked to everyone but the auditor and the org admin
    ColumnRule("email", "email", _ALL, frozenset({"auditor", "org_admin"}), masked=True, pii=False),
    # tagged pii, so restricted whoever sees it unmasked
    ColumnRule("ssn", "ssn", _ALL, frozenset(), masked=False, pii=True),
    # not visible to the analyst
    ColumnRule("salary", "salary", frozenset({"org_admin", "auditor"}), frozenset(), False, False),
]


def _results() -> dict[str, list[dict]]:
    cols = ["id", "email", "ssn", "salary"]
    return {
        "runs": [{"run_id": "r", "row_count": 4}],
        "columns": [
            {
                "run_id": "r",
                "run_time": "t",
                "column_name": c,
                "physical_column": c,
                "data_type": "varchar",
                "family": "text",
                "row_count": 4,
                "null_share": 0.0,
                "distinct_ratio": 1.0,
                "min_value": f"{c}-min",
                "max_value": f"{c}-max",
                "length_min": 1,
                "length_max": 9,
            }
            for c in cols
        ],
        "top_values": [{"column_name": c, "value": f"{c}-v"} for c in cols],
        "quantiles": [{"column_name": c, "measure": "length", "value": 3} for c in cols],
        "plausible_type": [{"column_name": c, "plausible_type": "unknown"} for c in cols],
        "fanout_runs": [{"relationship": "lines", "parents": 4}],
    }


def test_the_runs_view_is_safe_for_its_viewer():
    shown = safe_run(_results(), RULES, frozenset({"analyst"}))
    cols = {r["column_name"]: r for r in shown["columns"]}
    # salary is not visible to the analyst: omitted everywhere.
    assert set(cols) == {"id", "email", "ssn"}
    assert {r["column_name"] for r in shown["plausible_type"]} == {"id", "email", "ssn"}
    # id is shown in full.
    assert cols["id"]["min_value"] == "id-min" and cols["id"]["row_count"] == 4
    # email (masked to the analyst) and ssn (pii) show their shape only.
    for c in ("email", "ssn"):
        assert cols[c]["min_value"] is None and cols[c]["row_count"] is None
        assert (cols[c]["null_share"], cols[c]["distinct_ratio"], cols[c]["length_max"]) == (
            0.0,
            1.0,
            9,
        )
    assert {r["column_name"] for r in shown["top_values"]} == {"id"}
    assert {r["column_name"] for r in shown["quantiles"]} == {"id"}
    # Table-level kinds are untouched.
    assert shown["fanout_runs"] == _results()["fanout_runs"]


def test_a_viewer_unmasked_to_a_column_sees_its_values_but_never_a_pii_columns():
    shown = safe_run(_results(), RULES, frozenset({"auditor"}))
    assert {r["column_name"] for r in shown["top_values"]} == {"id", "email", "salary"}
    ssn = next(r for r in shown["columns"] if r["column_name"] == "ssn")
    assert ssn["max_value"] is None


def test_one_of_a_viewers_roles_unmasked_is_enough():
    shown = safe_run(_results(), RULES, frozenset({"analyst", "auditor"}))
    assert "email" in {r["column_name"] for r in shown["top_values"]}


def test_a_value_tables_default_rules_follow_each_described_column():
    defaults = prefill("top_values", list(field_names("top_values")), RULES)
    assert all(
        c["visibleTo"] == sorted(_ALL) and c["maskType"] is None for c in defaults["columns"]
    )
    rules = {r["roleId"]: r["filter"] for r in defaults["rowRules"]}
    assert rules == {
        "analyst": "column_name IN ('id')",
        "auditor": "column_name IN ('id', 'email', 'salary')",
        "org_admin": "column_name IN ('id', 'email', 'salary')",
    }


def test_the_columns_tables_value_fields_are_masked_to_readers_with_a_restricted_column():
    defaults = prefill("columns", list(field_names("columns")), RULES)
    by = {c["name"]: c for c in defaults["columns"]}
    # Every reader sees a pii column, so the value fields are masked (to NULL) for all of them.
    assert by["min_value"]["maskType"] == "constant" and by["min_value"]["unmaskedTo"] == []
    assert by["null_share"]["maskType"] is None
    rules = {r["roleId"]: r["filter"] for r in defaults["rowRules"]}
    # The analyst does not see salary, so its description is withheld from the analyst.
    assert rules == {"analyst": "column_name IN ('id', 'email', 'ssn')"}


def test_a_reader_with_no_restricted_column_sees_the_columns_values():
    rules = [r for r in RULES if r.physical in ("id", "email")]
    defaults = prefill("columns", list(field_names("columns")), rules)
    by = {c["name"]: c for c in defaults["columns"]}
    assert by["mean"]["unmaskedTo"] == ["auditor", "org_admin"]


def test_table_level_kinds_take_no_row_rule():
    assert prefill("runs", list(field_names("runs")), RULES)["rowRules"] == []
    assert prefill("fanout", list(field_names("fanout")), RULES)["rowRules"] == []
