# Copyright (c) 2026 Kenneth Stott
# Canary: e4a81d37-6c20-4f59-b3e7-02c9d6f15b78
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""postgres:17.11, started here, still answers system_corpus as pg17_system_answers.py recorded it
(REQ-2171): this is where that file comes from, since no PostgreSQL runs on a worker's machine. A
difference fails with the whole file as it should read, in base64, between RECORDING markers."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import base64
import importlib
import subprocess
import time
from collections.abc import Iterator

import psycopg
import pytest

from provisa.transpiler.pg_forms_enum import marked_source_casts, with_source_enums
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.pg17_everyday_function_answers import RECORDED
from tests.unit.pgwire.record_pg17_function_rules import _answer
from tests.unit.pgwire.record_pg17_system_answers import ANSWERS, recorded, written
from tests.unit.pgwire.test_pg_enum_forms import ENUM_STATEMENTS, ENUMS

pytestmark = [pytest.mark.integration]

_IMAGE = "postgres:17.11"
_READY_S = 120


@pytest.fixture(scope="module")
def postgres_17() -> Iterator[str]:
    started = subprocess.run(
        ["docker", "run", "-d", "-e", "POSTGRES_PASSWORD=pg17", "-p", "127.0.0.1::5432", _IMAGE],
        check=True,
        capture_output=True,
        text=True,
    )
    container = started.stdout.strip()
    try:
        port = (
            subprocess.run(
                ["docker", "port", container, "5432/tcp"],
                check=True,
                capture_output=True,
                text=True,
            )
            .stdout.split(":")[-1]
            .strip()
        )
        dsn = f"postgresql://postgres:pg17@127.0.0.1:{port}/postgres"
        deadline = time.monotonic() + _READY_S
        while True:
            try:
                psycopg.connect(dsn).close()
                break
            except psycopg.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        yield dsn
    finally:
        subprocess.run(["docker", "rm", "-f", container], check=True, capture_output=True)


def test_postgres_answers_as_recorded(postgres_17: str):
    answers = recorded(postgres_17)
    module = "tests.unit.pgwire.pg17_system_answers"
    on_file = importlib.import_module(module).RECORDED if ANSWERS.exists() else None
    encoded = base64.b64encode(written(answers).encode()).decode()
    assert on_file == answers, f"RECORDING-BEGIN {encoded} RECORDING-END"


def test_enum_forms_answer_on_the_postgresql_that_holds_the_type(postgres_17: str):
    """What a direct route sends a source: the forms and casts of pg_forms_enum, as it reads them.
    In a database of its own: the recording above is of a server in its default state."""
    with psycopg.connect(postgres_17, autocommit=True) as server:
        server.execute("CREATE DATABASE enums")
    with psycopg.connect(postgres_17.rsplit("/", 1)[0] + "/enums", autocommit=True) as source:
        source.execute("CREATE SCHEMA conformance")
        source.execute("CREATE TYPE conformance.probe_enum AS ENUM ('a', 'b', 'c')")
        wrong = {}
        for sql in ENUM_STATEMENTS:
            enum_written = marked_source_casts(with_source_enums(sql, ENUMS), ENUMS)
            written_sql = transpile(enum_written, "postgres")
            got = _answer(source, written_sql)
            if got[0] != "ok" or got[2] != RECORDED[sql][2]:
                wrong[sql] = f"{got} of {written_sql}"
    assert not wrong
