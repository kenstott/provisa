# Copyright (c) 2026 Kenneth Stott
# Canary: 3c58ea7f-91b0-4d26-8a41-0e7fd934cb62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: a real checker scans a real postgres table (REQ-1443).

Both checkers are pyproject EXTRAS — a Provisa install carries neither unless the operator chose one
(``install.sh`` writes ``dq_checker:``, ``scripts/provisa`` turns it into ``PROVISA_EXTRAS``). So the
test provisions what the shipped install provisions: a venv with the extra's packages in it, and the
worker run under that interpreter. Nothing is installed into the environment running the tests.

The venv is built once and cached under ``~/.cache`` — not /tmp, which a restart clears.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import asyncpg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
VENV_DIR = Path.home() / ".cache" / "provisa-dq-checkers"
CHECKER_PACKAGES = ["soda-postgres", "great-expectations[postgresql]", "pyyaml"]

SCHEMA = "dq_scan_it"
TABLE = "orders"

SODA_CONTRACT = f"""
dataset: provisa/{SCHEMA}/{TABLE}
columns:
  - name: id
    checks:
      - missing:
      - duplicate:
  - name: customer
    checks:
      - missing:
checks:
  - row_count:
      must_be_greater_than: 0
"""

GX_SUITE = json.dumps(
    {
        "name": "orders_suite",
        "meta": {"dataset": f"provisa/{SCHEMA}/{TABLE}"},
        "expectations": [
            {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "customer"}},
            {"type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
        ],
    }
)


@pytest.fixture(scope="session")
def checker_python() -> str:
    """A venv carrying both checker extras, built the way the shipped install builds them."""
    python = VENV_DIR / ("Scripts" if os.name == "nt" else "bin") / "python"
    if not python.exists():
        VENV_DIR.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True)
        subprocess.run(
            [str(python), "-m", "pip", "install", "--quiet", *CHECKER_PACKAGES], check=True
        )
    return str(python)


async def _seed(connection: dict, statements: list[str]) -> None:
    conn = await asyncpg.connect(
        host=connection["host"],
        port=connection["port"],
        database=connection["database"],
        user=connection["user"],
        password=connection["password"],
    )
    try:
        for statement in statements:
            await conn.execute(statement)
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def scanned_table(docker_postgres) -> Iterator[dict]:
    """A table with one known defect: exactly one null ``customer`` out of three rows."""
    connection = {
        "host": docker_postgres["host"],
        "port": docker_postgres["port"],
        "database": os.environ.get("PG_DATABASE", "provisa"),
        "user": os.environ.get("PG_USER", "provisa"),
        "password": os.environ.get("PG_PASSWORD", "provisa"),
    }
    asyncio.run(
        _seed(
            connection,
            [
                f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE",
                f"CREATE SCHEMA {SCHEMA}",
                f"CREATE TABLE {SCHEMA}.{TABLE} "
                f"(id int PRIMARY KEY, customer varchar, amount numeric)",
                f"INSERT INTO {SCHEMA}.{TABLE} VALUES (1,'a',10),(2,NULL,20),(3,'c',NULL)",
            ],
        )
    )
    yield connection
    asyncio.run(_seed(connection, [f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"]))


def _run_worker(checker_python: str, payload: dict, tmp_path: Path) -> dict:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(payload), encoding="utf-8")
    completed = subprocess.run(
        [checker_python, "-m", "provisa.dq.worker", str(payload_path)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        timeout=900,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def _rows(envelope: dict) -> list[dict]:
    from datetime import UTC, datetime

    from provisa.dq.runner import parse_results

    return parse_results(
        envelope,
        scan_id="scan-it",
        scan_time=datetime.now(UTC),
        dataset=f"provisa/{SCHEMA}/{TABLE}",
        target_table=f"{SCHEMA}.{TABLE}",
    )


@pytest.mark.parametrize(
    ("checker", "contract", "failing_check"),
    [
        ("soda", SODA_CONTRACT, "missing"),
        ("great_expectations", GX_SUITE, "expect_column_values_to_not_be_null"),
    ],
    ids=["soda", "great_expectations"],
)
def test_a_checker_scan_lands_one_row_per_check(
    checker_python, scanned_table, tmp_path, checker, contract, failing_check
):
    envelope = _run_worker(
        checker_python,
        {
            "checker": checker,
            "contract_text": contract,
            "connection": scanned_table,
            "data_source_name": "provisa",
            "sampler_limit": 20,
        },
        tmp_path,
    )
    assert envelope["checker"] == checker
    assert envelope["checker_version"]
    rows = _rows(envelope)
    assert len(rows) == len(envelope["checks"])

    # The seeded defect is one null customer out of three rows, and every checker must see it.
    failures = [r for r in rows if r["outcome"] == "fail"]
    assert [(r["column_name"], r["check_type"]) for r in failures] == [("customer", failing_check)]
    assert failures[0]["failed_rows"] == 1
    assert failures[0]["rows_tested"] == 3
    assert failures[0]["dataset"] == f"provisa/{SCHEMA}/{TABLE}"
    assert failures[0]["target_table"] == f"{SCHEMA}.{TABLE}"

    # Everything else passed — a checker that reported nothing would also produce no failures.
    assert {r["outcome"] for r in rows} == {"pass", "fail"}


def test_a_soda_scan_pushes_down_rather_than_pulling_the_dataset(
    checker_python, scanned_table, tmp_path
):
    """Soda emits aggregate SQL and reads back a row of scalars, which is what lets a scan cover a
    dataset far larger than memory. Observable here: the envelope reports counts over all three rows
    while carrying no value FROM any of them — and soda takes no failing-row samples at all without
    Soda Cloud, so even the failing check names no customer."""
    envelope = _run_worker(
        checker_python,
        {
            "checker": "soda",
            "contract_text": SODA_CONTRACT,
            "connection": scanned_table,
            "data_source_name": "provisa",
            "sampler_limit": 20,
        },
        tmp_path,
    )
    counted = [c for c in envelope["checks"] if c["rows_tested"] is not None]
    assert counted and all(c["rows_tested"] == 3 for c in counted)
    serialised = json.dumps(envelope)
    assert "'a'" not in serialised and '"a"' not in serialised and '"c"' not in serialised
