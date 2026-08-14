# Copyright (c) 2026 Kenneth Stott
# Canary: b16d3f80-4a72-4e59-9c08-27ad5e6f1b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests: checker result normalisation and the shipped results schema (REQ-1443).

``parse_results`` lives on Provisa's side of the subprocess boundary precisely so it is testable with
neither checker installed — these tests feed it the envelope the worker emits.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime

import pytest

from provisa.dq.results import (
    DQ_PROMOTIONS,
    DQ_WATERMARK_COLUMN,
    RESULT_FIELDS,
    results_columns,
)
from provisa.dq.runner import CheckerError, build_command, parse_results, run_contract

SCAN_TIME = datetime(2026, 8, 13, 9, 30, tzinfo=UTC)

# A minimal well-formed contract per dialect: enough for the runner's own dataset checks to pass so
# the run reaches the subprocess, which is what these tests are about.
_CONTRACTS = {
    "soda": "dataset: provisa/sales/orders\ncolumns: []\n",
    "great_expectations": "meta:\n  dataset: provisa/sales/orders\nexpectations: []\n",
}


def _envelope(**overrides) -> dict:
    check = {
        "column_name": "customer",
        "check_name": "No missing values",
        "check_type": "missing",
        "check_definition": "missing:",
        "outcome": "FAILED",
        "metric_value": 1,
        "threshold": "must be: 0",
        "rows_tested": 3,
        "failed_rows": 1,
        "diagnostics": {"missing_count": 1, "dataset_rows_tested": 3},
    }
    check.update(overrides)
    return {"checker": "soda", "checker_version": "4.20.0", "checks": [check]}


def _parse(envelope: dict) -> list[dict]:
    return parse_results(
        envelope,
        scan_id="scan-1",
        scan_time=SCAN_TIME,
        dataset="provisa/sales/orders",
        target_table="sales.orders",
    )


def test_a_row_has_exactly_the_shipped_fields_in_order():
    """The landing path writes the table's declared columns, so a row that gained or lost a key
    would be a silent schema drift."""
    (row,) = _parse(_envelope())
    assert tuple(row) == RESULT_FIELDS


def test_scan_identity_comes_from_the_caller_not_the_checker():
    (row,) = _parse(_envelope())
    assert row["scan_id"] == "scan-1"
    assert row["scan_time"] == SCAN_TIME
    assert row["dataset"] == "provisa/sales/orders"
    assert row["target_table"] == "sales.orders"


def test_checker_identity_is_carried_through_from_the_envelope():
    (row,) = _parse(_envelope())
    assert (row["checker"], row["checker_version"]) == ("soda", "4.20.0")


@pytest.mark.parametrize(
    ("reported", "landed"),
    [
        ("PASSED", "pass"),
        ("FAILED", "fail"),
        ("WARN", "warn"),
        ("NOT_EVALUATED", "error"),
        ("EXCLUDED", "skipped"),
    ],
)
def test_every_checker_outcome_maps_to_a_shipped_outcome(reported: str, landed: str):
    (row,) = _parse(_envelope(outcome=reported))
    assert row["outcome"] == landed


def test_an_unknown_outcome_fails_loudly():
    """A vocabulary the mapping does not know is a checker-version change, not a row to guess at."""
    with pytest.raises(CheckerError, match="unknown outcome"):
        _parse(_envelope(outcome="MAYBE"))


def test_a_check_with_no_numeric_metric_lands_null_not_zero():
    """0.0 would read as a measurement that was taken; a schema check measures no number at all."""
    (row,) = _parse(_envelope(metric_value=None, rows_tested=None, failed_rows=None))
    assert row["metric_value"] is None
    assert row["rows_tested"] is None
    assert row["failed_rows"] is None


def test_numeric_fields_are_coerced_to_their_column_types():
    (row,) = _parse(_envelope(metric_value=1, rows_tested=3.0, failed_rows=1.0))
    assert isinstance(row["metric_value"], float)
    assert (row["rows_tested"], row["failed_rows"]) == (3, 1)


@pytest.mark.parametrize("bad", ["1", True, {"n": 1}])
def test_a_non_numeric_metric_is_rejected(bad):
    with pytest.raises(CheckerError, match="expected a numeric"):
        _parse(_envelope(metric_value=bad))


def test_diagnostics_pass_through_whole():
    (row,) = _parse(_envelope())
    assert row["diagnostics"] == {"missing_count": 1, "dataset_rows_tested": 3}


def test_an_envelope_without_checker_identity_is_rejected():
    envelope = _envelope()
    del envelope["checker_version"]
    with pytest.raises(CheckerError, match="checker_version"):
        _parse(envelope)


def test_the_worker_runs_in_the_same_interpreter_as_a_module():
    """The isolation being bought is IMPORT isolation, not environment isolation: the checker is a
    pyproject extra installed into this environment."""
    assert build_command("/tmp/p.json") == [sys.executable, "-m", "provisa.dq.worker", "/tmp/p.json"]


@pytest.mark.asyncio
async def test_an_unknown_checker_never_reaches_a_subprocess():
    with pytest.raises(CheckerError, match="unknown data-quality checker"):
        await run_contract(
            checker="deequ",
            contract_text="dataset: provisa/sales/orders",
            connection={},
            data_source_name="provisa",
            scan_id="s",
            scan_time=SCAN_TIME,
            target_table="sales.orders",
        )


@pytest.mark.asyncio
async def test_a_contract_naming_a_different_data_source_is_reported_not_corrected():
    with pytest.raises(CheckerError, match="must agree"):
        await run_contract(
            checker="soda",
            contract_text="dataset: elsewhere/sales/orders",
            connection={},
            data_source_name="provisa",
            scan_id="s",
            scan_time=SCAN_TIME,
            target_table="sales.orders",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("checker", "extra"), [("great_expectations", "gx"), ("soda", "soda")]
)
async def test_an_uninstalled_checker_reports_the_extra_that_supplies_it(checker, extra):
    """A checker is acquired on operator selection, so "not installed" is an ordinary operator state
    and the failure has to name its own remedy rather than surface the checker's import traceback."""
    if importlib.util.find_spec("great_expectations" if extra == "gx" else "soda_core"):
        pytest.skip(f"the {extra} extra is installed in this environment")
    with pytest.raises(CheckerError, match=f"pip install 'provisa\\[{extra}\\]'"):
        await run_contract(
            checker=checker,
            contract_text=_CONTRACTS[checker],
            connection={},
            data_source_name="provisa",
            scan_id="s",
            scan_time=SCAN_TIME,
            target_table="sales.orders",
        )


def test_the_shipped_schema_is_what_parse_results_emits():
    assert tuple(c.name for c in results_columns(["*"])) == RESULT_FIELDS


def test_scan_time_is_the_watermark_so_scans_accumulate():
    """probe_type=watermark implies APPEND (REQ-982) — the results table is a scan history with no
    history subsystem."""
    assert DQ_WATERMARK_COLUMN in RESULT_FIELDS
    (column,) = [c for c in results_columns(["*"]) if c.name == DQ_WATERMARK_COLUMN]
    assert column.data_type == "timestamp"


def test_governance_is_the_callers_not_a_default_invented_here():
    assert all(c.visible_to == ["analyst"] for c in results_columns(["analyst"]))


def test_shipped_promotions_name_fields_the_runner_actually_writes():
    """A promotion of a key nothing emits would be a column that is null forever."""
    emitted = {"max_timestamp", "dataset_rows_tested"}
    assert {p["field"] for p in DQ_PROMOTIONS} <= emitted
    assert all(p["jsonb_column"] == "diagnostics" for p in DQ_PROMOTIONS)
