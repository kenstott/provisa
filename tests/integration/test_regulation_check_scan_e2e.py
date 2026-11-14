# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6b3f84-9e21-4a57-b8c3-2f7e1a5d9c60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A kept regulation check is run by the real Soda checker (REQ-2245).

The check :func:`provisa.dq.regulation.regulation_check` keeps -- a ``failed_rows`` check whose
expression is the condition and whose qualifier is its identity -- is put in a contract beside
another check and a second kept condition by the one contract builder, and run by Soda Core through
the shipped worker against tables in the test stack's Postgres. It passes on rows none of which
meet the condition and fails on rows some of which do.

The checker is provisioned as the shipped install provisions it, in a cached venv
(``test_dq_checker_scan``); nothing is installed into the environment running the tests.
"""

# Requirements: REQ-2245

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterator

import pytest

from provisa.dq import regulation
from provisa.dq.catalog import build_check_definition
from provisa.dq.contract import build_contract
from tests.integration.test_dq_checker_scan import _run_worker, _seed, checker_venv_python

pytestmark = [pytest.mark.integration]

SCHEMA = "regulation_check_it"
_MISSING = {
    "column_name": "email",
    "check_type": "missing",
    "definition": build_check_definition("soda", "missing", column_name="email"),
}
_NO_AT = "email NOT LIKE '%@%'"
_EXAMPLE = "email LIKE '%@example.com'"


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
    asyncio.run(
        _seed(
            connection,
            [
                f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE",
                f"CREATE SCHEMA {SCHEMA}",
                # No row meets either condition.
                f"CREATE TABLE {SCHEMA}.people_ok (id int, email varchar)",
                f"INSERT INTO {SCHEMA}.people_ok VALUES (1, 'a@corp.org'), (2, 'b@corp.org')",
                # One row has no @ and one is at example.com.
                f"CREATE TABLE {SCHEMA}.people_bad (id int, email varchar)",
                f"INSERT INTO {SCHEMA}.people_bad VALUES (1, 'nobody'), (2, 'c@example.com')",
            ],
        )
    )
    yield connection
    asyncio.run(_seed(connection, [f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"]))


def test_a_scan_runs_the_kept_check(checker_python, tables, tmp_path):
    checks = [
        _MISSING,
        regulation.regulation_check(
            regulation.regulation_qualifier(1, "email", _NO_AT, "section:1:heading:x"), _NO_AT
        ),
        regulation.regulation_check(
            regulation.regulation_qualifier(1, "email", _EXAMPLE, "section:1:heading:x"), _EXAMPLE
        ),
    ]
    for table, expected in (("people_ok", "PASSED"), ("people_bad", "FAILED")):
        contract = build_contract("soda", f"provisa/{SCHEMA}/{table}", checks)
        assert len(regulation.held_qualifiers(contract)) == 2
        envelope = _run_worker(
            checker_python,
            {
                "checker": "soda",
                "contract_text": contract,
                "connection": tables,
                "data_source_name": "provisa",
                "sampler_limit": 5,
            },
            tmp_path,
        )
        said = json.dumps(envelope)
        outcomes = sorted((r["check_type"], r["outcome"]) for r in envelope["checks"])
        # The other check passes on both; each kept condition fails where a row meets it.
        assert outcomes == [
            ("failed_rows", expected),
            ("failed_rows", expected),
            ("missing", "PASSED"),
        ], (table, said)
