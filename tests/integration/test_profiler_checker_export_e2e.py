# Copyright (c) 2026 Kenneth Stott
# Canary: a83d5f20-6c47-4e19-b2a5-0f9c7e1d3b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The checks a profile exports, run by the real checkers (REQ-1934, REQ-1443).

Every check :mod:`provisa.profiler.export` writes -- each accepted-constraint kind (the ordering as
Soda ``failed_rows`` and as the GX column-pair expectation, a date range as SQL), the drift check
and the expectation check -- is put in a contract by the one contract builder and run by Soda Core
and by Great Expectations through the shipped worker, against tables in the test stack's Postgres.
Each check passes on data that meets it and fails on data that breaks it.

The checkers are provisioned as the shipped install provisions them, in a cached venv
(``test_dq_checker_scan``); nothing is installed into the environment running the tests.
"""

# Requirements: REQ-1934, REQ-1443

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from provisa.dq.contract import build_contract
from provisa.profiler import export
from provisa.profiler.constraints import Constraint
from tests.integration.test_dq_checker_scan import _run_worker, _seed, checker_venv_python

pytestmark = [pytest.mark.integration]

SCHEMA = "profiler_export_it"
CHECKERS = ("soda", "great_expectations")

_CONSTRAINTS = [
    Constraint("not_null", "id", None, {}),
    Constraint("unique", "id", None, {}),
    Constraint("value_set", "status", None, {"values": ["new", "paid"]}),
    Constraint("range", "amount", None, {"min": 0, "max": 100, "family": "numeric"}),
    Constraint(
        "range",
        "placed",
        None,
        {
            "min": 0,
            "max": 1,
            "min_text": "2026-01-01",
            "max_text": "2026-12-31",
            "family": "temporal",
        },
    ),
    # placed is never after shipped.
    Constraint("ordering", "placed", "shipped", {"family": "temporal"}),
]

_DRIFT_COLUMNS = (
    "run_id text, run_time timestamptz, scope text, column_name text, measure text, "
    "current double precision, drifting boolean"
)
_EXPECTATION_COLUMNS = (
    '"measure" text, "column" text, period_start timestamptz, period_end timestamptz, '
    "low double precision, expected double precision, high double precision, source text"
)


@pytest.fixture(scope="session")
def checker_python() -> str:
    return checker_venv_python()


@pytest.fixture(scope="session")
def tables(docker_postgres) -> Iterator[dict]:
    connection = {
        "host": docker_postgres["host"],
        "port": docker_postgres["port"],
        "database": os.environ.get("PG_DATABASE", "provisa"),
        "user": os.environ.get("PG_USER", "provisa"),
        "password": os.environ.get("PG_PASSWORD", "provisa"),
    }
    orders = "id int, status varchar, amount int, placed date, shipped date"
    asyncio.run(
        _seed(
            connection,
            [
                f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE",
                f"CREATE SCHEMA {SCHEMA}",
                # Every constraint holds.
                f"CREATE TABLE {SCHEMA}.orders_ok ({orders})",
                f"INSERT INTO {SCHEMA}.orders_ok VALUES "
                "(1, 'new', 10, '2026-02-01', '2026-02-02'), "
                "(2, 'paid', 20, '2026-03-01', '2026-03-01')",
                # Every constraint but not_null is broken: shipped before placed, a repeated id, a
                # status not recorded, an amount and a date out of range.
                f"CREATE TABLE {SCHEMA}.orders_bad ({orders})",
                f"INSERT INTO {SCHEMA}.orders_bad VALUES "
                "(1, 'new', 10, '2026-02-01', '2026-01-31'), "
                "(1, 'sent', 200, '2027-01-01', '2027-01-02')",
                # Drift tables of the shipped drift shape (the columns the checks read): an older
                # run drifted in both, only drift_alarm's latest run does.
                f"CREATE TABLE {SCHEMA}.drift_calm ({_DRIFT_COLUMNS})",
                f"INSERT INTO {SCHEMA}.drift_calm VALUES "
                "('r1', '2026-05-01T03:00:00Z', 'table', NULL, 'row_count', 400, TRUE), "
                "('r2', '2026-05-02T03:00:00Z', 'table', NULL, 'row_count', 100, FALSE)",
                f"CREATE TABLE {SCHEMA}.drift_alarm ({_DRIFT_COLUMNS})",
                f"INSERT INTO {SCHEMA}.drift_alarm VALUES "
                "('r1', '2026-05-01T03:00:00Z', 'table', NULL, 'row_count', 100, FALSE), "
                "('r2', '2026-05-02T03:00:00Z', 'table', NULL, 'row_count', 400, TRUE)",
                # Expectations of the published shape for drift_calm's latest run (row_count 100):
                # one holds it, one does not. Each also covers the older run (400), which an
                # expectation check of the latest run must leave alone.
                f"CREATE TABLE {SCHEMA}.expect_met ({_EXPECTATION_COLUMNS})",
                f"INSERT INTO {SCHEMA}.expect_met VALUES "
                "('row_count', NULL, '2026-05-01T00:00:00Z', '2026-06-01T00:00:00Z', "
                "90, 100, 110, 'plan')",
                f"CREATE TABLE {SCHEMA}.expect_missed ({_EXPECTATION_COLUMNS})",
                f"INSERT INTO {SCHEMA}.expect_missed VALUES "
                "('row_count', NULL, '2026-05-01T00:00:00Z', '2026-06-01T00:00:00Z', "
                "300, 400, 500, 'forecast')",
            ],
        )
    )
    yield connection
    asyncio.run(_seed(connection, [f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"]))


def _checker_table(checker: str, table: str) -> export.CheckerTable:
    dataset = f"provisa/{SCHEMA}/{table}"
    return export.CheckerTable(1, f"{table}_checks", "src", checker, dataset, "")


def _run(python: str, tmp_path: Path, conn: dict, target: export.CheckerTable, checks) -> list:
    contract = build_contract(target.checker, target.dataset, list(checks))
    envelope = _run_worker(
        python,
        {
            "checker": target.checker,
            "contract_text": contract,
            "connection": conn,
            "data_source_name": "provisa",
            "sampler_limit": 5,
        },
        tmp_path,
    )
    results = envelope["checks"]
    assert len(results) == len(list(checks)), envelope
    # Every check ran: none is an error the checker could not evaluate.
    assert all(r["outcome"] in ("PASSED", "FAILED") for r in results), json.dumps(envelope)
    return results


@pytest.mark.parametrize("checker", CHECKERS)
def test_every_constraint_kind_passes_on_data_meeting_it_and_fails_on_data_breaking_it(
    checker_python, tables, tmp_path, checker
):
    for table, expected in (
        ("orders_ok", ["PASSED"] * 6),
        ("orders_bad", ["PASSED"] + ["FAILED"] * 5),
    ):
        target = _checker_table(checker, table)
        checks = [export.constraint_check(c, checker) for c in _CONSTRAINTS]
        results = _run(checker_python, tmp_path, tables, target, checks)
        by_type = [r["check_type"] for r in results]
        assert [r["outcome"] for r in results] == expected, (table, by_type, results)
    # The ordering reaches each checker as its own kind of check.
    ordering = export.constraint_check(_CONSTRAINTS[-1], checker)["check_type"]
    assert ordering == (
        "failed_rows" if checker == "soda" else "expect_column_pair_values_a_to_be_greater_than_b"
    )


@pytest.mark.parametrize("checker", CHECKERS)
def test_the_drift_check_fails_only_when_the_latest_run_drifts(
    checker_python, tables, tmp_path, checker
):
    for table, outcome in (("drift_calm", "PASSED"), ("drift_alarm", "FAILED")):
        target = _checker_table(checker, table)
        [result] = _run(checker_python, tmp_path, tables, target, [export.drift_check(target)])
        assert result["outcome"] == outcome, (table, result)


@pytest.mark.parametrize("checker", CHECKERS)
def test_the_expectation_check_holds_the_latest_run_to_its_periods_bounds(
    checker_python, tables, tmp_path, checker
):
    target = _checker_table(checker, "drift_calm")
    for expectations, outcome in (("expect_met", "PASSED"), ("expect_missed", "FAILED")):
        check = export.expectation_check(target, f"{SCHEMA}.{expectations}")
        [result] = _run(checker_python, tmp_path, tables, target, [check])
        assert result["outcome"] == outcome, (expectations, result)
